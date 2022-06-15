FROM datamechanics/spark:3.1-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

USER root

RUN apt-get update && \
    apt-get -y install gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/application

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY main.py .
COPY jobs/ jobs/
COPY datasets/ datasets/
COPY test/ test/
COPY .pylintrc .
