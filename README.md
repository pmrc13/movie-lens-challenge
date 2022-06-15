# asos-movie-lens

This repository contains a proposed solution to ASOS' Big Data Engineer Code Test.
It was developed using the PySpark framework, the Python's testing library unittest, and Docker.

# Building and running the solution with venv

Install venv

````bash
python3.9 -m pip install --user virtualenv
````

Create a virtual environment using venv

````bash
python3.9 -m venv venv
````

Activate the virtual environment

````bash
source venv/bin/activate
````

Install the required packages

````bash
pip install -r requirements.txt
````

Run the `main.py` file

````bash
python jobs/main.py
````

Run unit tests

````bash
python -m unittest test.staging_test.StagingTestCase
python -m unittest test.transformation_test.TransformationTestCase
````

Run static analysis

````bash
pylint jobs
pylint test
````

Deactivate the virtual environment

````bash
deactivate
````

# Building and running the solution with Docker

Build the Docker image

````bash
docker build -t asos-movie-lens .
````

Run the jobs

````bash
docker run asos-movie-lens driver local:///opt/application/jobs/main.py
````

Run unit tests

````bash
docker run asos-movie-lens python -m unittest test.staging_test.StagingTestCase
docker run asos-movie-lens python -m unittest test.transformation_test.TransformationTestCase
````

Run static analysis

````bash
docker run asos-movie-lens pylint jobs
docker run asos-movie-lens pylint test
````
