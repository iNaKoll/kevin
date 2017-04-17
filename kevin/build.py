"""
Build code

A build is a repo state that was triggered to be run in multiple jobs.
"""

from pathlib import Path
import shutil
import time

from .config import CFG
from .job import Job
from .project import Project
from .update import (BuildState, BuildSource, QueueActions, JobState,
                     JobCreated, JobUpdate, ActionsAttached,
                     Update, GeneratedUpdate, JobEmergencyAbort)
from .watcher import Watchable, Watcher


class Build(Watchable, Watcher):
    """
    A Build is a request to process a specific state of a repo, identified
    by the commit hash. The build then launches multiple jobs, as required
    by the associated project.

    The Jobs are then executed on some falk instance, where all the steps
    for the job are run.
    """

    def __init__(self, project, commit_hash):
        super().__init__()

        if not (commit_hash.isalnum() and len(commit_hash) == 40):
            raise ValueError("bad commit SHA: " + repr(commit_hash))

        self.commit_hash = commit_hash

        if not isinstance(project, Project):
            raise TypeError("invalid project type: %s" % type(project))
        self.project = project

        # No more jobs required to perform for this build.
        # If the build has been completed, stores the unix timestamp (float).
        # else, it is None.
        self.completed = None

        # Was the finish() function called?
        self.finished = False

        # The Queue where this build was put in
        self.queue = None

        # Special information storage, for example the status update url,
        # or other custom used by triggers and actions.
        self.info = dict()

        # gathered sources of this build.
        self.sources = set()

        # List of build status update JSON objects.
        # Job specific updates are stored in the appropriate job.
        self.updates = list()

        # jobs required for this build to succeeed.
        # job_name -> Job
        self.jobs = dict()

        # job status collections
        self.jobs_pending = set()
        self.jobs_succeeded = set()
        self.jobs_errored = set()

        # URL where the repo containing this commit can be cloned from
        # during the 'building' phase.
        self.clone_url = None

        # folder/project/jobs/hash[:3]/hash/
        self.relpath = Path(
            self.project.name,
            "jobs",
            self.commit_hash[:3],  # -> 4096 folders with 2**148 files max
            self.commit_hash[3:]
        )

        # storage path for the job output
        self.path = CFG.output_folder / self.relpath

        # info url of this build
        mandy_url = "%s?wsurl=ws://%s:%d/ws&staticurl=%s&project=%s&hash=%s"
        self.target_url = mandy_url % (
            CFG.mandy_url,
            CFG.dyn_host,
            CFG.dyn_port,
            CFG.static_url,
            self.project.name,
            self.commit_hash
        )

        # try to reconstruct the build state from filesystem
        # when reconstruction is possible, this also reconstructs jobs.
        self.load_from_fs()

        # create the output folder
        if not CFG.args.volatile:
            if not self.path.is_dir():
                self.path.mkdir(parents=True)

        # add jobs and other actions defined by the project
        # some jobs may already be attached by the reconstruction.
        self.project.attach_actions(self)

        # tell all watchers (e.g. jobs) that they were attached,
        # and may now register themselves at this build.
        self.send_update(ActionsAttached())

    def load_from_fs(self):
        """
        set up this build from the filesystem.
        """

        # only reconstruct if we wanna use the local storage
        if CFG.args.volatile:
            return

        # Check if the build was completed already.
        try:
            self.completed = self.path.joinpath("_completed").stat().st_mtime
        except FileNotFoundError:
            pass

        if self.completed is not None:
            with self.path.joinpath("_updates").open() as updates_file:
                for json_line in updates_file:
                    self.send_update(Update.construct(json_line),
                                     reconstruction=True)
        else:
            # make sure that there are no remains
            # of previous aborted build.
            try:
                shutil.rmtree(str(self.path))
            except FileNotFoundError:
                pass

    def set_state(self, state, text, timestamp=None):
        """ set this build state """
        self.send_update(BuildState(self.project.name, self.commit_hash,
                                    state, text, timestamp))

    def add_source(self, clone_url, repo_url=None, user=None, branch=None,
                   comment=None):
        """
        Store the build source settings, namely the repo url.
        """
        # a primitive duplicate-source filter
        for source in self.sources:
            if source.repo_url == repo_url:
                return

        self.send_update(BuildSource(
            clone_url=clone_url,   # Where to clone the repo from
            repo_url=repo_url,     # Website of the repo
            author=user,           # User that triggered the build
            branch=branch,         # Branchname of this build
            comment=comment,
        ))

    def enqueue_actions(self, queue):
        """
        The actions of this build must now add themselves to the
        given queue.
        """

        # memorize the queue
        self.queue = queue

        # if loaded from fs, we know if the build was completed.
        if self.completed is None:
            # TODO send more fine-grained build progress states
            self.set_state("waiting", "enqueued")

        # notify all watchers (e.g. jobs) that they should run.
        # jobs use this as the signal to reconstruct themselves,
        # or, if they're "new" jobs, to enqueue their execution.
        self.send_update(QueueActions(self.commit_hash, queue,
                                      self.project.name))

    def register_job(self, job):
        """
        Registers a job that is run for this build.

        This is called from the Job when we send
        the `ActionsAttached` update.
        """

        if job.name in self.jobs:
            # the job is already registered if the build is reconstructed
            # and a job in the reconstruction was attached by
            # project settings previously!
            return

        # some job was notified by this build and now
        # says "hey i'm created now."
        self.jobs[job.name] = job

        # put it into pending, even if it's actually finished.
        # we'll soon get a JobState update which will put
        # it into the right queue.
        self.jobs_pending.add(job)

    def on_watch(self, watcher):
        """
        Registers a watcher object to this build.

        The watcher's on_update() member method will be called for every
        update that ever was and ever will be until unwatch() below is
        called.
        """
        # send all previous updates to the watcher
        for update in self.updates:
            watcher.on_update(update)

    def on_send_update(self, update, reconstruction=False):
        """ Called before this update is sent to all watchers. """

        # if the update is stored in the update list that
        # is sent to a new subscriber.
        record = True

        # if reconstructing, we don't need to save anything to disk
        store_to_disk = not reconstruction

        # don't serialize generated updates to disk
        # when we'll reconstruct from disk,
        # those will be generated again.
        if isinstance(update, GeneratedUpdate):
            store_to_disk = False

        elif isinstance(update, BuildSource):
            self.sources.add(update)
            self.clone_url = update.clone_url

        elif isinstance(update, JobCreated):

            # recreate all jobs that were active when the
            # build ran.
            if reconstruction and update.job_name not in self.jobs:

                job = Job(self, self.project,
                          update.job_name, update.vm_name)

                # subscribe the job to build's updates.
                # that way, the job will get the ActionsAttached update
                # and the job will register at this build.
                self.watch(job)

        if record:
            self.updates.append(update)

        if not store_to_disk or CFG.args.volatile:
            # don't write the update to the job storage
            return

        # whitelist for stored build updates
        if not isinstance(update, (BuildSource, JobCreated)):
            return

        # append this update to the build updates file
        # TODO perf: don't open _updates on each update!
        with self.path.joinpath("_updates").open("a") as ufile:
            ufile.write(update.json() + "\n")

    def on_update(self, update):
        """
        Received message from somewhere,
        now relay it to watchers that may want to see it.
        """

        # shall we relay the message to the watchers of the build?
        distribute = False

        if isinstance(update, JobCreated):
            if self.finished:
                raise Exception("job created after build was finished!")

        # all job updates are distributed.
        if isinstance(update, JobUpdate):
            distribute = True

            if isinstance(update, JobEmergencyAbort):
                # only send the update if it was not a emergency abort!
                # this prevents that an exception in this call prevents
                # reaching the finish stuff below.
                distribute = False

        # send the update e.g. to mandy
        if distribute:
            # exclude jobs from receiving the update
            self.send_update(
                update,
                lambda subscriber: isinstance(subscriber, Job)
            )

        # track job state updates:
        if isinstance(update, JobState):
            # TODO: if one step of a job failed,
            # the build must wait until the remaining steps are run.
            if update.job_name not in self.jobs:
                raise Exception("unknown state update for job '%s' "
                                "in project '%s'" % (update.job_name,
                                                     self.project.name))

            # a job reports its status:
            job = self.jobs[update.job_name]

            if job in self.jobs_pending:
                if update.is_finished():
                    self.jobs_pending.remove(job)

                if update.is_succeeded():
                    self.jobs_succeeded.add(job)

                if update.is_errored():
                    self.jobs_errored.add(job)
            else:
                # update for a non-pending job.
                # this happens e.g. for further failure
                # notifications from chantal.
                pass

            if not self.jobs_pending:
                self.finish()

    def finish(self):
        """ no more jobs are pending  """
        if self.finished:
            # finish message already sent.
            return

        if self.queue is not None:
            self.queue.remove_build(self)

            # we're no longer enqueued.
            self.queue = None

        try:
            # TODO: we may wanna have allowed-to-fail jobs.
            if self.jobs_succeeded == set(self.jobs.values()):
                count = len(self.jobs)
                self.set_state("success", "%d job%s succeeded" % (
                    count, "s" if count > 1 else ""))

            else:
                # we had some unsucessful jobs:

                if self.jobs_errored:
                    # one or more jobs errored.
                    count = len(self.jobs_errored)
                    self.set_state("error", "%d job%s errored" % (
                        count, "s" if count > 1 else ""))

                else:
                    # one or more jobs failed.
                    count = len(self.jobs)
                    self.set_state("failure", "%d/%d job%s failed" % (
                        count - len(self.jobs_succeeded),
                        count, "s" if count > 1 else ""))
        finally:
            # build is completed now!
            if not CFG.args.volatile:
                self.path.joinpath("_completed").touch()
            self.completed = time.time()
            self.finished = True

    def abort(self):
        """ Abort this build """

        if self.finished:
            return

        if self.queue is not None:
            for job in self.jobs_pending.copy():
                self.queue.cancel_job(job)

        self.finish()
