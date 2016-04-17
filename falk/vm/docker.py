"""
Docker containers.

https://www.docker.com/
"""

import logging
import os
from pathlib import Path

from . import Container, ContainerConfig
import docker


class Docker(Container):
    """
    Represents a Docker container.
    """

    def __init__(self, cfg):
        super().__init__(cfg)
        self.manage = False
        self.docker_container_id = None

    @classmethod
    def config(cls, machine_id, cfgdata, cfgpath):
        cfg = ContainerConfig(machine_id, cfgdata, cfgpath)

        cfg.dockerfile = Path(cfgdata.get("dockerfile", "Dockerfile"))
        if not cfg.dockerfile.is_absolute():
            cfg.dockerfile = cfgpath / dockerfile
        
        cfg.image_name = cfgdata.get("image_name")
        assert bool(cfg.image_name is not None) != cfg.dockerfile.exists(), "Falk docker provider configuration error, at least a docker file or an image name is required"
        cfg.docker_client = docker.Client(base_url=cfgdata.get("docker_socket_uri", "unix://var/run/docker.sock"))

        if not cfg.docker_file.is_file():
            raise FileNotFoundError("Dockerfile: %s" % cfg.dockerfile)

        return cfg

    async def prepare(self, manage=False):
        self.manage = manage

        if not self.manage:
            # build the image
            self.cfg.docker_client.build(path=str(self.cfg.dockerfile), rm=True, tag="chantal/{}".format(self.cfg.image_name))

    async def launch(self):
        container = self.cfg.docker_client.create_container(image="chantal/{}".format(self.cfg.image_name), command='/usr/bin/python3 -m chantal')
        self.docker_container_id = container['Id']

    async def is_running(self):
        running_container_ids = list(map(lambda container: container['Id'], self.cfg.docker_client.containers()))
        return self.docker_container_id in running_container_ids

    async def terminate(self):
        if self.docker_container_id is not None:
            self.cfg.docker_client.kill(self.docker_container_id)
            self.cfg.docker_client.wait(self.docker_container_id)

    async def cleanup(self):
        if self.docker_container_id is not None:
            self.cfg.docker_client.remove_container(self.docker_container_id)
            self.docker_container_id = None
