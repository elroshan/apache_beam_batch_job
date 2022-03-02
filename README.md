# Intorduction

A data pipeline that ingests data from gs://cloud-samples-data/bigquery/sample-transactions in CSV format.
Transforms data using Apache Beam and stores the result under output folder.

# Implementation overview

The solution consists of a containerized Python application which reads, transforms and writes data to a local file,
compressed in jsonl.gz format.


# Prerequisites
To install and deploy, you need to have Docker Desktop.

# Configuration steps

Navigate to the root of the project and run the following command:

`docker build . image_name`

This will build the image for you. To run the image, execute the following command:

In Windows: `docker run -v C:\path\to\project_folder:/home/beam_batch_job image_name:latest`

In Linux: `docker run -v ``pwd``/:/home/beam_batch_job image_name:latest`

# Notes 
Here some high level notes on how the solution was implemented is shared.

## Tasks list

- Get the docker app "working", prints on screen.
- Get the app to read the data , prints on screen.
- Transform data based on the specifications.
- Update the transformation and use 'Composite Transform' instead.
- Write Unit test for the Composite Transform.
