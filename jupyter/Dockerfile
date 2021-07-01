FROM jupyter/pyspark-notebook

USER root
WORKDIR /home/jovyan/work

COPY project_data.csv .
COPY processing.ipynb .
COPY processing.py .

RUN pip install pykafka
RUN pip install kafka-python

