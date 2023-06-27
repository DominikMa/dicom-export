FROM ubuntu:latest
RUN apt-get update && apt-get -y install dcmtk python3 python3-pip build-essential
RUN pip3 install pydantic