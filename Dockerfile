from ubuntu:14.04

RUN apt-get update && \
	apt-get install -y \
	wget software-properties-common openssh-server git apt-transport-https ca-certificates && \
	apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D && \
	echo deb https://apt.dockerproject.org/repo ubuntu-trusty main > /etc/apt/sources.list.d/docker.list && \
	add-apt-repository ppa:fkrull/deadsnakes && \
	apt-get update && \
	apt-get install -y \
	docker-engine python3.5 && \
	wget https://bootstrap.pypa.io/get-pip.py && \
	python3.5 get-pip.py && rm get-pip.py && \
	pip3.5 install docker-py tornado requests

WORKDIR /kevin
RUN git clone https://github.com/iNaKoll/kevin.git .
RUN python3.5 setup.py install

