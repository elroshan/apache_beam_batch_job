 # syntax=docker/dockerfile:1
 FROM python:3.9-slim-buster
 RUN pip install wheel
 RUN pip install apache-beam[gcp]
 RUN mkdir /home/beam_batch_job
 COPY . /home/beam_batch_job
 WORKDIR /home/beam_batch_job/src
 CMD [ "python3", "main.py"]