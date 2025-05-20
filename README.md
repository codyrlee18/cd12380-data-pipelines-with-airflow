# Data Pipelines with Airflow

This project uses Apache Airflow to create a data pipeline for the Sparkify music streaming platform. The pipeline extracts JSON data from S3, stages it in Amazon Redshift, and transforms it into a star schema for efficient analysis. The DAG is designed to be reusable and maintainable, following best practices for data engineering.

## To Begin

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

Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`. Don't forget to start your Redshift cluster via the AWS console.  In the Airflow UI, you should see a DAG named final_project_dag. This DAG is pre-configured to handle the entire ETL process, including:

- Staging data from S3 to Redshift
- Loading fact and dimension tables
- Running data quality checksAfter completing these steps,

You can trigger the DAG manually from the Airflow UI once the environment is up and the Redshift cluster is active. All tasks should execute successfully.

The create_tables.sql file in this repository has already been run to set up the necessary tables in the Redshift dev database, so this step should not be needed by the evaluator. The tables include:

- artists
- songplays
- songs
- staging_events
- staging_songs
- time
- users

  This is what the DAG graph will look like:

![Project DAG in the Airflow UI](assets/final_project_dag_graph2.png)

## DAG Configuration

This step was completed succesfully. I used generative AI (ChatGPT) to create the custom operators for this project. I also referred to Airflow open-source code and documentation to guide me through the DAG creation process. I used the base templates from the project instructions and then iteratively implemented the generated code with guidance - this allowed me to better understand the structure and functionaloity of the Airflow operators as I went on to implement them. 

In the DAG, the set up is based on these project guidelines:
* No dependencies on past runs.
* Tasks are retried three times on failure.
* Retries occur every five minutes.
* Catchup is turned off.
* No email on retry.
