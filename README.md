# Data Pipelines with Airflow

This project uses Apache Airflow to create a data pipeline for the Sparkify music streaming platform. The pipeline extracts JSON data from S3, stages it in Amazon Redshift, and transforms it into a star schema for efficient analysis. The DAG is designed to be reusable and maintainable, following best practices for data engineering.

### To Begin

- Docker installed and running

- AWS Redshift cluster set up (e.g., the serverless default-workgroup)

- AWS credentials configured in Airflow

- The S3 bucket containing the required log-data and song-data for the project (mine is clearly referenced in the code as my-udacity-bucket-codylee)
  

## Initiating the Airflow Web Server

To get the Airflow containers up and running, run the following command from the root directory of this repository:

```bash
docker-compose up -d
```
This command will spin up the Airflow UI, scheduler, worker, and other required services. Once the containers are running, you can access the Airflow UI at:

```bash
http://localhost:8080
```
On the Airflow web server UI, use `airflow` for both username and password.

## Getting set up

The create_tables.sql file in this repository has already been run to set up the necessary tables in the Redshift dev database, so this step should not be needed by the evaluator. The tables include:

- artists
- songplays
- songs
- staging_events
- staging_songs
- time
- users

* Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
* Don't forget to start your Redshift cluster via the AWS console.
* After completing these steps, run the DAG to ensure all tasks are successfully executed.

## Getting Started with the Project

In the Airflow UI, you should see a DAG named final_project_dag. This DAG is pre-configured to handle the entire ETL process, including:

- Staging data from S3 to Redshift

- Loading fact and dimension tables

- Running data quality checks

You can trigger the DAG manually from the Airflow UI once the environment is up and the Redshift cluster is active.


With these files in place, you should see the DAG in the Airflow UI, with a graph view like the one below:
![Project DAG in the Airflow UI](assets/final_project_dag_graph2.png)
You should be able to execute the DAG successfully.

## DAG Configuration
In the DAG, the `default parameters` are based on these guidelines:
* No dependencies on past runs.
* Tasks are retried three times on failure.
* Retries occur every five minutes.
* Catchup is turned off.
* No email on retry.

The below was completed succesfully. I used generative AI such as ChatGPT to create the custom operators for this project. Using the base templates from the project instructions and then iteratively implemented them with guidance - this allowed me to better understand the structure and functionaloity of the Airflow operators as I went on to implement them. 

## Developing Operators
To complete the project, build four operators for staging data, transforming data, and performing data quality checks. While you can reuse code from Project 2, leverage Airflow's built-in functionalities like connections and hooks whenever possible to let Airflow handle the heavy lifting.

### Stage Operator
Load any JSON-formatted files from S3 to Amazon Redshift using the stage operator. The operator should create and run a SQL COPY statement based on provided parameters, distinguishing between JSON files. It should also support loading timestamped files from S3 based on execution time for backfills.

### Fact and Dimension Operators
Utilize the provided SQL helper class for data transformations. These operators take a SQL statement, target database, and optional target table as input. For dimension loads, implement the truncate-insert pattern, allowing for switching between insert modes. Fact tables should support append-only functionality.

### Data Quality Operator
Create the data quality operator to run checks on the data using SQL-based test cases and expected results. The operator should raise an exception and initiate task retry and eventual failure if test results don't match expectations.
