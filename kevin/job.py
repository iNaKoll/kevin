"""
Job processing code
"""

import asyncio
import json
import logging
import shutil
import time as clock
import traceback

from .chantal import Chantal
from .config import CFG
from .falk import FalkSSH, FalkSocket
from .falkvm import VMError
from .process import (ProcTimeoutError, LineReadError, ProcessFailed,
                      ProcessError)
from .update import (Update, JobState, StepState,
                     StdOut, OutputItem, Enqueued, JobCreated,
                     GeneratedUpdate, ActionsAttached, JobEmergencyAbort)
from .util import recvcoroutine
from .watcher import Watcher, Watchable
from .service import Action



class JobTimeoutError(Exception):
    """
    Indicates a job execution timeout.
    """
    def __init__(self, timeout, was_global):
        super().__init__()
        self.timeout = timeout
        self.was_global = was_global


class JobAction(Action):
    """
    This action attaches a new job to a build.
    """

    @classmethod
    def name(cls):
        return "job"

    def __init__(self, cfg, project):
        super().__init__(cfg, project)
        self.job_name = cfg["name"]
        self.descripton = cfg.get("description")
        self.vm_name = cfg["machine"]

    def get_watcher(self, build):
        return Job(build, self.project, self.job_name, self.vm_name)


class Job(Watcher, Watchable):
    """
    Holds all info for one job, which runs a commit SHA of a project
    in a falk machine.

    TODO: when "restarting" the job, the reconstruction from fs must
          not happen. for that, a "reset" must be implemented.
    """
    def __init__(self, build, project, name, vm_name):
        super().__init__()

        # the project and build this job is invoked by
        self.build = build
        self.project = project

        # name of this job within a build.
        self.name = name

        # name of the vm where the job shall run.
        self.vm_name = vm_name

        # No more tasks to perform for this job?
        # If the job is completed, stores the completion timestamp (float).
        # else, stores None.
        self.completed = None

        # the tasks required to run for this job.
        self.tasks = set()

        # List of job status update JSON objects.
        self.updates = list()

        # Internal cache, used when erroring all remaining pending states.
        self.pending_steps = set()

        # {step name: step number}, used for step name prefixes
        self.step_numbers = dict()

        # all commited output items
        self.output_items = set()

        # current uncommited output item
        self.current_output_item = None

        # current step name of the job
        self.current_step = None

        # remaining size limit
        self.remaining_output_size = self.project.cfg.job_max_output

        # receive files from chantal
        self.raw_file = None
        self.raw_remaining = 0

        # storage folder for this job.
        self.path = build.path.joinpath(self.name)

        # tell the our watchers that we enqueued ourselves
        self.send_update(JobCreated(self.build.commit_hash, self.name))

        # try to reconstruct from the persistent storage.
        self.load_from_fs()

        # create the output directory structure
        if not CFG.args.volatile:
            if not self.path.is_dir():
                self.path.mkdir(parents=True)

    def load_from_fs(self):
        """
        reconstruct the job from the filesystem.
        TODO: currently, the old job attempt is deleted if not finished.
        maybe we wanna keep it.
        """

        # only reconstruct if we wanna use the local storage
        if CFG.args.volatile:
            return

        # Check if the job was completed.
        try:
            self.completed = self.path.joinpath("_completed").stat().st_mtime
        except FileNotFoundError:
            pass

        if self.completed is not None:
            # load update list from file
            with self.path.joinpath("_updates").open() as updates_file:
                for json_line in updates_file:
                    self.send_update(Update.construct(json_line),
                                     save=True, fs_store=False,
                                     forbid_completed=False)

        else:
            # make sure that there are no remains
            # of previous aborted jobs.
            try:
                shutil.rmtree(str(self.path))
            except FileNotFoundError:
                pass

    def __str__(self):
        return "%s.%s [\x1b[33m%s\x1b[m]" % (
            self.build.project.name,
            self.name,
            self.build.commit_hash)

    async def get_falk_vm(self, vm_name):
        """
        return a suitable vm instance for this job from a falk.
        """

        machine = None

        # try each falk to find the machine
        for falkname, falkcfg in CFG.falks.items():

            # TODO: better selection if this falk is suitable, e.g. has
            #       machine for this job. either via cache or direct query.
            #
            # TODO: allow falk bypass by launching VM locally without a
            #       falk daemon!

            falk = None

            if falkcfg["connection"] == "ssh":
                host, port = falkcfg["location"]
                falk = FalkSSH(host, port, falkcfg["user"],
                               falkcfg["key"])

            elif falkcfg["connection"] == "unix":
                falk = FalkSocket(falkcfg["location"], falkcfg["user"])

            else:
                raise Exception("unknown falk connection type: %s -> %s" % (
                    falkname, falkcfg["connection"]))

            try:
                # connect
                await falk.create()

                # create container
                machine = await falk.create_vm(vm_name)

            except (LineReadError, ProcessError) as exc:
                # TODO: connection rejections, auth problems, ...
                logging.warning("failed communicating "
                                "with falk '%s'", falkname)
                logging.warning("\x1b[31merror\x1b[m: $ %s",
                                " ".join(exc.cmd))
                logging.warning("       -> %s", exc)
                logging.warning("  are you sure %s = '%s' "
                                "is a valid falk entry?",
                                falkname,
                                falk)

            if machine is not None:
                # we found the machine
                return machine

        if machine is None:
            raise VMError("VM '%s' could not be provided by any falk" % (
                vm_name))

    def on_send_update(self, update, save=True, fs_store=True,
                       forbid_completed=True):
        """
        When an update is to be sent to all watchers
        """

        if update == StopIteration:
            return

        if forbid_completed and self.completed is not None:
            raise Exception("job sending update after being completed.")

        # when it's a StepUpdate, this manages the pending_steps set.
        update.apply_to(self)

        if isinstance(update, GeneratedUpdate):
            save = False

        if save:
            self.updates.append(update)

        if not save or not fs_store or CFG.args.volatile:
            # don't write the update to the job storage
            return

        # append this update to the build updates file
        with self.path.joinpath("_updates").open("a") as ufile:
            ufile.write(update.json() + "\n")

    def on_update(self, update):
        """
        When this job receives updates from any of its watched
        watchables, the update is processed here.
        """

        if isinstance(update, ActionsAttached):
            # tell the build that we're an assigned job.
            self.build.register_job(self)

            # we are already attached to receive updates from a build
            # now, we subscribe the build to us so it gets our updates.
            # when we reconstructed the job from filesystem,
            # this step feeds all the data into the build.
            self.watch(self.build)

        elif isinstance(update, Enqueued):
            if self.completed is None:
                # add the job to the processing queue
                update.queue.add_job(self)

    def step_update(self, update):
        """ apply a step update to this job. """

        if not isinstance(update, StepState):
            raise Exception("tried to use non-StepState to step_update")

        if update.state == "pending":
            self.pending_steps.add(update.step_name)
        else:
            try:
                self.pending_steps.remove(update.step_name)
            except KeyError:
                pass

        if update.step_number is None:
            if update.step_name not in self.step_numbers:
                self.step_numbers[update.step_name] = len(self.step_numbers)
            update.step_number = self.step_numbers[update.step_name]

    def set_state(self, state, text, time=None):
        """ set the job state information """
        self.send_update(JobState(self.project.name, self.build.commit_hash,
                                  self.name, state, text, time))


    def set_step_state(self, step_name, state, text, time=None):
        """ send a StepState update. """
        self.send_update(StepState(self.project.name, self.build.commit_hash,
                                   self.name, step_name, state, text,
                                   time=time))

    def on_watch(self, watcher):
        # send all previous job updates to the watcher
        for update in self.updates:
            watcher.on_update(update)

        # and send stop if this job is finished
        if self.completed is not None:
            watcher.on_update(StopIteration)


    async def run(self):
        """ Attempts to build the job. """

        try:
            if self.completed is not None:
                raise Exception("tried to run a completed job!")

            # falk contact
            self.set_state("pending", "requesting machine")

            machine = await asyncio.wait_for(self.get_falk_vm(self.vm_name),
                                             timeout=10)

            # machine was acquired, now boot it.
            self.set_state("pending", "booting machine")

            async with Chantal(machine) as chantal:

                # wait for the machine to be reachable
                await chantal.wait_for_connection(timeout=60, try_timeout=20)

                # install chantal (someday, might be preinstalled)
                await chantal.install(timeout=20)

                # create control message parser sink
                control_handler = self.control_handler()

                try:
                    # run chantal process in the machine
                    async with chantal.run(self) as run:

                        # and fetch all the output
                        async for stream_id, data in run.output():
                            # stdout message:
                            if stream_id == 1:
                                self.send_update(
                                    StdOut(self.project.name,
                                           self.build.commit_hash,
                                           self.name,
                                           data.decode("utf-8",
                                                       errors="replace")))

                            # control message stream chunk:
                            elif stream_id == 2:
                                control_handler.send(data)

                        # wait for chantal termination
                        await run.wait()

                except ProcTimeoutError as exc:
                    raise JobTimeoutError(exc.timeout, exc.was_global)

        except asyncio.CancelledError:
            logging.info("\x1b[31;1mJob aborted:\x1b[m %s", self)
            self.error("Job cancelled")
            raise

        except (ProcessFailed, asyncio.TimeoutError) as exc:
            if isinstance(exc, ProcessFailed):
                error = ": %s" % exc.cmd[0]
                what = "failed"
            else:
                error = ""
                what = "timed out"

            logging.error("[job] \x1b[31;1mProcess %s:\x1b[m %s:\n%s",
                          what, self, exc)

            self.error("Process failed%s" % error)

        except (LineReadError, ProcessError) as exc:
            logging.error("[job] \x1b[31;1mCommunication failure:"
                          "\x1b[m %s", self)

            logging.error(" $ %s: %s", " ".join(exc.cmd), exc)

            self.error("Process communication error: %s" % (exc.cmd[0]))

        except ProcTimeoutError as exc:
            if exc.was_global:
                silence = "Took"
            else:
                silence = "Silence time"

            logging.error("[job] \x1b[31;1mTimeout:\x1b[m %s", self)
            logging.error(" $ %s", " ".join(exc.cmd))
            logging.error("\x1b[31;1m%s longer than limit "
                          "of %.2fs.\x1b[m", silence, exc.timeout)
            traceback.print_exc()

            self.error("Process %s took > %.02fs." % (exc.cmd[0],
                                                      exc.timeout))

        except JobTimeoutError as exc:

            # did it take too long to finish?
            if exc.was_global:
                logging.error("[job] \x1b[31;1mTimeout:\x1b[m %s", self)
                logging.error("\x1b[31;1mTook longer than limit "
                              "of %.2fs.\x1b[m", exc.timeout)

                if self.current_step:
                    self.set_step_state(self.current_step, "error",
                                        "Timeout!")

                self.error("Job took > %.02fs." % (exc.timeout))

            # or too long to provide a message?
            else:
                logging.error("[job] \x1b[31;1mSilence timeout!\x1b[m "
                              "%s\n\x1b[31mQuiet for > %.2fs.\x1b[m",
                              self, exc.timeout)

                # a specific step is responsible:
                if self.current_step:
                    self.set_step_state(self.current_step, "error",
                                        "Silence for > %.02fs." % (
                                            exc.timeout))
                    self.error("Silence Timeout!")
                else:
                    # bad step is unknown:
                    self.error("Silence for > %.2fs!" % (exc.timeout))

        except VMError as exc:
            logging.error("\x1b[31;1mMachine action failed\x1b[m "
                          "%s.%s [\x1b[33m%s\x1b[m]",
                          self.build.project.name,
                          self.name,
                          self.build.commit_hash)
            traceback.print_exc()

            self.error("VM Error: %s" % (exc))

        except Exception as exc:
            logging.error("\x1b[31;1mexception in Job.run()\x1b[m "
                          "%s.%s [\x1b[33m%s\x1b[m]",
                          self.build.project.name,
                          self.name,
                          self.build.commit_hash)
            traceback.print_exc()

            try:
                # make sure the job dies
                self.error("Job.run(): %r" % (exc))
            except Exception as exc:
                # we end up here if the error update can not be sent,
                # because one of our watchers has a programming error.
                # some emergency handling is required.

                logging.error(
                    "\x1b[31;1mjob failure status update failed again! "
                    "Performing emergency abort, waaaaaah!\x1b[m")
                traceback.print_exc()

                try:
                    # make sure the job really really dies
                    self.send_update(
                        JobEmergencyAbort(
                            self.project.name,
                            self.build.commit_hash,
                            self.name,
                            "Killed job after double fault of job."
                        )
                    )
                except Exception as exc:
                    logging.error(
                        "\x1b[31;1mOk, I give up. The job won't die. "
                        "I'm so Sorry.\x1b[m")
                    traceback.print_exc()

        finally:
            # error the leftover steps
            for step in self.pending_steps:
                self.set_step_state(step, 'error',
                                    'step result was not reported')

            # the job is completed!
            self.completed = clock.time()

            if not CFG.args.volatile:
                # the job is now officially completed
                self.path.joinpath("_completed").touch()

            self.send_update(StopIteration)

    def error(self, text):
        """
        Produces an 'error' JobState and an 'error' StepState for all
        steps that are currently pending.
        """
        while self.pending_steps:
            step = self.pending_steps.pop()
            self.set_step_state(step, "error", "build has errored")

        self.set_state("error", text)

    @recvcoroutine
    def control_handler(self):
        """
        Coroutine that receives control data chunks via yield, and
        interprets them.
        Note: The data chunks are entirely untrusted.
        """
        data = bytearray()

        while True:

            # transfer files through raw binary mode
            if self.raw_file is not None:
                if not data:
                    # wait for more data
                    data += yield

                if len(data) >= self.raw_remaining:
                    # the file is nearly complete,
                    # the remaining data is the next control message

                    self.raw_file.write(data[:self.raw_remaining])
                    self.raw_file.close()
                    self.raw_file = None

                    del data[:self.raw_remaining]

                else:
                    # all the data currently buffered shall go into the file
                    self.raw_file.write(data)
                    self.raw_remaining -= len(data)
                    del data[:]

                continue

            # control stream is in regular JSON+'\n' mode
            newline = data.find(b"\n")
            if newline < 0:
                if len(data) > (8 * 1024 * 1024):
                    # chantal is trying to crash us with a >= 8MiB msg
                    raise ValueError("Control message too long")

                # wait for more data
                data += yield
                continue

            # parse the line as control message,
            # after the newline may be raw data or the next control message
            msg = data[:newline + 1].decode().strip()
            if msg:
                self.control_message(msg)

            del data[:newline + 1]

    def control_message(self, msg):
        """
        control message parser, chantal sends state through this channel.
        """

        msg = json.loads(msg)

        cmd = msg["cmd"]

        if cmd == 'job-state':
            self.set_state(msg["state"], msg["text"])

        elif cmd == 'step-state':
            self.current_step = msg["step"]
            self.set_step_state(msg["step"], msg["state"], msg["text"])

        # finalize file transfer
        elif cmd == 'output-item':
            name = msg["name"]

            if self.current_output_item is None:
                raise ValueError("no data received for " + name)
            if self.current_output_item.name != name:
                raise ValueError(
                    "wrong output item name: " + name + ", "
                    "expected: " + self.current_output_item.name
                )

            self.send_update(self.current_output_item)
            self.current_output_item = None

        # file or folder transfer is announced
        elif cmd in {'output-dir', 'output-file'}:
            path = msg["path"]

            if not path:
                raise ValueError("invalid path: is empty")

            if path[0] in {".", "_"}:
                raise ValueError("invalid start of output filename: . or _")

            if '/' in path:
                # a file with / is emitted, it must be some subdirectory/file
                # -> ensure this happens as part of an output item.
                if self.current_output_item is None:
                    raise ValueError("no current output item")
                self.current_output_item.validate_path(path)

            else:
                if self.current_output_item is not None:
                    raise ValueError("an output item is already present")

                self.current_output_item = OutputItem(
                    self.name,
                    path,
                    isdir=(cmd == 'output-dir')
                )

            if cmd == 'output-file':
                # prevent attackers from using negative integers/floats
                size = abs(int(msg["size"]))
            else:
                size = 0

            # also account for metadata size
            # (prevent DOSers from creating billions of empty files)
            self.current_output_item.size += (size + 512)
            if self.current_output_item.size > self.remaining_output_size:
                raise ValueError("output size limit exceeded")

            pathobj = self.path.joinpath(path)
            if pathobj.exists():
                raise ValueError("duplicate output path: " + path)

            self.raw_remaining = size

            if CFG.args.volatile:
                logging.warning("'%s' ignored because of "
                                "volatile mode active: %s", cmd, pathobj)
                self.raw_file = open("/dev/null", 'wb')

            elif cmd == 'output-file':
                self.raw_file = pathobj.open('wb')

            elif cmd == 'output-dir':
                pathobj.mkdir(parents=True, exist_ok=True)

            else:
                raise ValueError("unknown command: {}".format(cmd))

        else:
            raise ValueError("unknown build control command: %r" % (cmd))
