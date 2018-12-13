"""
Docker containers.

https://www.docker.com/
"""

from pathlib import Path
import asyncio
import logging
import io
import os
import shlex
import tarfile
import uuid

from .base import Container, ContainerConfig
from kevin.util import INF
import docker


class _DockerProcess:

    def __init__(self, dockerc_base, container_id, cmd,
                 stdin=False, stdout=True, stderr=True,
                 height=None, width=None):

        self._dockerc_base = dockerc_base
        self._container_id = container_id
        self.cmd = cmd
        tty = stdin
        self._exec_res = self._dockerc_base.exec_create(
            self._container, cmd, tty=tty,
            stdin=stdin, stdout=stdout, stderr=stderr
        )
        self._exec_id = self._exec_res["Id"]
        self._dockerc_base.exec_resize(self._exec_id, height=height, width=width)
        self._web_socket = self._dockerc_base.exec_start(self._exec_id, tty=tty, socket=True)
        self._inspect()

    def _inspect(self):
        self._inspect_res = self._client.exec_inspect(self._exec_id)

    def poll(self):
        self._inspect()

    def wait(self):
        raise NotImplementedError

    def communicate(self):
        raise NotImplementedError

    @property
    def stdin(self):
        pass

    @property
    def stdout(self):
        pass

    @property
    def stderr(self):
        pass

    @property
    def returncode(self):
        pass

class DockerContainerConfig:
    """
    Configuration for a container.
    Guarantees the existence of:
     * Machine ID     (docker container id)
     * Machine Name   (to match for)

    Created from a config dict that contains key-value pairs.
    """
    def __init__(self, machine_id, cfg, cfgpath):

        # store the machine id
        self.machine_id = machine_id
        self.cfgpath = cfgpath

        config_keys = ("name", "ssh_user", "ssh_host", "ssh_port",
                       "ssh_known_host_key", "ssh_known_host_key_file")

        # set all config keys to None.
        for key in config_keys:
            setattr(self, key, None)

class Docker(Container):
    """
    Represents a Docker container.
    """

    def __init__(self, cfg):
        super().__init__(cfg)
        self.manage = False
        self.container = None

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
        cfg.dockerc = docker.Client(base_url=base_url)
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
                detach=True,
                auto_remove=True,
                stdin_open=True,
                tty=True,
            )

            logging.info("instance name : {}".format(self.container.name))
        else:
            # TODO: error ?
            pass

    async def execute(self, remote_command,
                      timeout=INF, silence_timeout=INF,
                      must_succeed=True):
        """
        Runs the command via ssh, returns an Process handle.
        """

        ws = self.cfg.dockerc_base.attach_socket(self.container.id, ws=True)

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
            await asyncio.get_event_loop().run_in_executor(
                None, self.container.start)
        else:
            # TODO: error ?
            pass

    async def is_running(self):
        if self.container is not None:
            self.container = await asyncio.get_event_loop().run_in_executor(
                self.cfg.dockerc.containers.get, self.container.id)
            return self.container.status == "running"
        else:
            return False

    async def terminate(self):
        if self.container is not None:
            await asyncio.get_event_loop().run_in_executor(
                None, self.cfg.dockerc_base.stop, self.container.id)

    async def cleanup(self):
        if self.container is not None:
            await asyncio.get_event_loop().run_in_executor(
                None, self.cfg.dockerc_base.remove_container, self.container.id)
            self.container = None


# tests
async def main():
    cfgdata = {"image_name": "ubuntu"}
    cfgpath = Path(__file__).parent / "tests"
    machine_id = "zorro"
    container = Docker(Docker.config(machine_id, cfgdata, cfgpath))
    await container.prepare()
    await container.launch()
    print("Container {} is running {}".format(
        container,
        await container.is_running()
    ))


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
