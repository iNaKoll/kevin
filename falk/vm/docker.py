"""
Docker containers.

https://www.docker.com/
"""

from asyncio import base_subprocess
from pathlib import Path
import asyncio
import concurrent
import logging
import io
import os
import shlex
import signal
import subprocess
import struct
import tarfile
import threading
import uuid

from .base import Container, ContainerConfig, DockerContainerConfig
from kevin.util import INF
import docker


class _DockerProcess:
    """
    A class representing a remote docker container process and implementing
    the subprocess.Popen class interface.
    """

    def __init__(self, loop, container, cmd, protocol, waiter, on_exit,
                 height=None, width=None):

        self._container = container
        self._dockerc_base = container.cfg.dockerc_base
        self._container_id = container.container.id
        self._cmd = cmd
        self._protocol = protocol
        self._docker_waiter = waiter
        self._on_exit = on_exit
        self._height = height or 600
        self._width = width or 800
        self._returncode = None
        self._loop = loop

        self._init_event = threading.Event()
        asyncio.ensure_future(
            self._loop.run_in_executor(None, self._do_init),
            loop=self._loop)
        self._init_event.wait()

    def _do_init(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._stdin_r, self._stdin_w = os.pipe()
            self._stdout_r, self._stdout_w = os.pipe()
            self._stderr_r, self._stderr_w = os.pipe()
            self._stdin_f = os.fdopen(self._stdin_w, mode="wb")
            self._stdout_f = os.fdopen(self._stdout_r, mode="rb")
            self._stderr_f = os.fdopen(self._stderr_r, mode="rb")
            self._streams = {
                1: self._stdout_w,
                2: self._stderr_w,
            }
            loop.add_reader(self._stdin_r, self._process_writer)

            self._exec_res = self._dockerc_base.exec_create(
                self._container_id, self._cmd, tty=False,
                stdin=True, stdout=True, stderr=True
            )
            self._exec_id = self._exec_res["Id"]
            # FIXME: exec_resize request throw an HTTP 500 error
            # self._dockerc_base.exec_resize(
            #     self._exec_id, height=self._height, width=self._width)
            self._socket = self._dockerc_base.exec_start(
                self._exec_id, detach=False, tty=False, stream=True, socket=True)._sock

            self._inspect_res = self._dockerc_base.exec_inspect(self._exec_id)
            self._pid = self._inspect_res["Pid"]
        except:
            # This is only useful for debugging
            logging.exception("")
            loop.close()
            raise
        finally:
            self._init_event.set()

        loop.run_until_complete(self._process_read())
        loop.close()

    def _inspect(self):
        self._inspect_res = self._dockerc_base.exec_inspect(self._exec_id)
        self._pid = self._inspect_res["Pid"]
        if not self._inspect_res["Running"]:
            self._returncode = self._inspect_res["ExitCode"]

    async def _process_read(self):
        data = self._socket.recv(4096)
        fd = -1
        frames = {
            1: b"",
            2: b""
        }
        stream_protocols = {
            1: self._protocol.stdout,
            2: self._protocol.stderr,
        }
        lenght = None
        try:
            while self._returncode is None:
                # Inspect container
                self._inspect()
                if not self._docker_waiter.done():
                    self._loop.call_soon_threadsafe(self._docker_waiter.set_result, None)

                # Read output and error streams
                if fd > 0:
                    missing = lenght - len(frames[fd])
                    frames[fd] += data[:missing]
                    data = data[missing:]
                    if len(frames[fd]) == lenght:
                        os.write(self._streams[fd], frames[fd])
                        self._loop.call_soon_threadsafe(
                            stream_protocols[fd].feed_data, frames[fd][:]
                        )
                        frames[fd] = b""
                        fd = - 1
                        lenght = None
                elif len(data) >= 8:
                    fd, lenght = struct.unpack_from(b">BxxxI", data[:8])
                    data = data[8:]

                if not data:
                    await asyncio.sleep(0.100)
                    chunk = self._socket.recv(4096)
                    data += chunk
        finally:
            self._loop.call_soon_threadsafe(self._protocol.stdout.feed_eof)
            self._loop.call_soon_threadsafe(self._protocol.stderr.feed_eof)
            self._loop.call_soon_threadsafe(self._on_exit, self._returncode)
            os.close(self._stdout_w)
            os.close(self._stderr_w)

    def _process_writer(self):
        data = os.read(self._stdin_r, 4096)
        while data:
            self._socket.send(os.read(self._stdin_r))
            data = os.read(self._stdin_r, 4096)

    def poll(self):
        asyncio.ensure_future(self._inspect(), loop=self._loop)
        # FIXME, synchronization issue here:
        # _inspect_res has not yet been updated by the _inspect call above
        # This might cause an undesirable delay in the exit code propagation
        if not self._inspect_res["Running"]:
            self._returncode = self._inspect_res["ExitCode"]
        return self._returncode

    def wait(self, timeout=None):
        """
        Not used by asyncio. To be implemended if needed.
        """
        raise NotImplementedError

    def communicate(input=None, timeout=None):
        """
        Not used by asyncio. To be implemended if needed.
        """
        raise NotImplementedError

    def send_signal(self, signal):
        asyncio.wait(asyncio.ensure_future(
            self._container.send_signal(signal, self._pid),
            loop=self._loop), loop=self._loop)

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)

    @property
    def args(self):
        return self._cmd

    @property
    def stdin(self):
        return self._stdin_f

    @property
    def stdout(self):
        return self._stdout_f

    @property
    def stderr(self):
        return self._stderr_f

    @property
    def pid(self):
        return self._pid

    @property
    def returncode(self):
        return self._returncode


class _DockerProcessTransport(base_subprocess.BaseSubprocessTransport):

    def __init__(self, loop, protocol, args, shell,
                 stdin, stdout, stderr, bufsize, dockerc_base, container,
                 waiter=None, extra=None, **kwargs):
        self.dockerc_base = dockerc_base
        self.container = container
        self._docker_waiter = waiter
        self._docker_loop = loop
        super(_DockerProcessTransport, self).__init__(
            loop, protocol, args, shell,
            stdin, stdout, stderr, bufsize,
            waiter=waiter, extra=extra, **kwargs)

    def _start(self, args, shell, stdin, stdout,
               stderr, bufsize, **kwargs):
        self._proc = _DockerProcess(
            self._docker_loop, self.container, args,
            self._protocol, self._docker_waiter, self._process_exited
        )


class Docker(Container):
    """
    Represents a Docker container.
    """

    def __init__(self, cfg):
        super().__init__(cfg)
        self.manage = False
        self.container = None
        self.socket = None

    @classmethod
    def config(cls, machine_id, cfgdata, cfgpath):
        cfg = DockerContainerConfig(machine_id, cfgdata, cfgpath)

        cfg.dockerfile = Path(cfgdata.get("dockerfile", "Dockerfile"))
        if not cfg.dockerfile.is_absolute():
            cfg.dockerfile = cfgpath / cfg.dockerfile

        cfg.image_name = cfgdata.get("image_name")
        assert (bool(cfg.image_name is not None) and cfg.dockerfile.exists()), \
            "Falk docker provider configuration error, " \
            "at least a docker file and an image name is required"

        base_url = cfgdata.get(
            "docker_socket_uri", "unix://var/run/docker.sock")
        cfg.dockerc = docker.DockerClient(base_url=base_url)
        cfg.dockerc_base = docker.APIClient(base_url=base_url)

        if not cfg.dockerfile.is_file():
            raise FileNotFoundError("Dockerfile: %s" % cfg.dockerfile)

        return cfg

    async def prepare(self, manage=False):
        logging.info("preparing instance...")
        if self.container is None:
            self.manage = manage

            if not self.manage:
                # build the image
                dockerfile_path = str(self.cfg.dockerfile)
                with open(dockerfile_path, 'rb') as dockerfile:
                    self.cfg.dockerc.images.build(
                        fileobj=dockerfile, rm=True, tag=self.cfg.image_name
                    )
            logging.info("instance image name : {}".format(self.cfg.image_name))

            # Create a container with a shell process waiting for its stdin
            container_name = "{}-{}".format(
                self.cfg.name, str(uuid.uuid4())[:8])
            self.container = self.cfg.dockerc.containers.create(
                name=container_name,
                image=self.cfg.image_name,
                command="/bin/bash",
                detach=False,
                auto_remove=True,
                stdin_open=True,
                tty=False,
            )

            logging.info("instance name : {}".format(self.container.name))
        else:
            # TODO: error ?
            pass

    async def execute(self, cmd,
                      timeout=INF, silence_timeout=INF,
                      must_succeed=True):
        """
        Runs the command via ssh, returns an Process handle.
        """
        loop = asyncio.get_event_loop()
        protocol = asyncio.subprocess.SubprocessStreamProtocol(limit=4096, loop=loop)
        waiter = loop.create_future()
        transport = _DockerProcessTransport(
            loop=loop, protocol=protocol, args=cmd, shell=False,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, bufsize=4096, waiter=waiter,
            dockerc_base=self.cfg.dockerc_base, container=self)
        try:
            await waiter
        except Exception:
            logging.exception("")
            transport.close()
            await transport._wait()
            raise
        return asyncio.subprocess.Process(
            transport=transport,
            protocol=protocol,
            loop=loop,
        )

    async def upload(self, local_path, remote_folder="/", timeout=10):
        """
        Uploads the file or directory from local_path to
        remote_folder (default: ~).
        """
        with io.BytesIO() as f:
            tar = tarfile.open(fileobj=f, mode="w:gz")
            tar.add(local_path)
            f.seek(0)

            def _do_upload():
                self.cfg.dockerc_base.put_archive(
                    self.container.id, remote_folder, f.read())

            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, _do_upload),
                timeout=timeout)

    async def download(self, remote_path, local_folder, timeout=10):
        """
        Downloads the file or directory from remote_path to local_folder.
        Warning: Contains no safeguards regarding filesize.
        Clever arguments for remote_path or local_folder might
        allow break-outs.
        """
        with io.BytesIO() as f:

            def _do_download():
                chunks, stat = self.cfg.dockerc_base.get_archive(
                    self.container.id, remote_path)
                for chunk in chunks:
                    f.write(chunk)

            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, _do_download),
                timeout=timeout)
            f.seek(0)
            tar = tarfile.open(fileobj=f, mode="r:*")
            tar.extract(Path(remote_path).name, path=local_folder)

    async def launch(self):
        if self.container is not None:
            await asyncio.get_event_loop().run_in_executor(None, self.container.start)
            self.socket = (await asyncio.get_event_loop().run_in_executor(
                None, self.cfg.dockerc_base.attach_socket, self.container.id,
                dict(stdin=True, stdout=True, stderr=True, stream=True), False))._sock
        else:
            # TODO: error ?
            pass

    async def is_running(self):
        if self.container is not None:
            self.container = await asyncio.get_event_loop().run_in_executor(
                None, self.cfg.dockerc.containers.get, self.container.id)
            return self.container.status == "running"
        else:
            return False

    async def send_signal(self, signal, pid):
        if self.container is not None:
            await asyncio.get_event_loop().run_in_executor(
                None, self.socket.send, "kill -{} {}".format(signal, pid).encode('utf-8'))

    async def terminate(self):
        if self.container is not None:
            # Send the detach sequence to the container.
            # This will close the stdin of the container process (/bin/cat) and
            # trigger a graceful exit.
            self.socket.send(b"\xA0\xA1")  # send ctrl+p ctrl+q

    async def wait_for_shutdown(self, timeout=60):
        """
        sleep for a maximum of `timeout` until the container terminates.
        """
        raise NotImplementedError

    async def cleanup(self):
        if self.container is not None:
            await asyncio.get_event_loop().run_in_executor(
                None, self.cfg.dockerc_base.remove_container, self.container.id)
            self.container = None


# Tests
# Usage:
#   python -m falk.vm.docker
async def main():
    cfgdata = {"type": "docker", "image_name": "ubuntu"}
    cfgpath = Path(__file__).parent / "tests"
    machine_id = "zorro"
    container = Docker(Docker.config(machine_id, cfgdata, cfgpath))
    await container.prepare()
    await container.launch()
    print("Container {} is running {}".format(
        container,
        await container.is_running()
    ))
    print("exectute...")
    p = await container.execute("ls -al")
    print("communicate...")
    out, err = await p.communicate()
    print("stdout:\n{}\n\nstderr:\n{}".format(
        out.decode('utf-8'), err.decode('utf-8')))
    await container.terminate()


if __name__ == "__main__":
    import warnings
    warnings.simplefilter('always', ResourceWarning)
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.5
    loop.run_until_complete(main())
