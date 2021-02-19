# Music-Trends-ETL--pipeline

## Data Engineering NanoDegree Capstone Project

### Project Summary
Music has changed a lot through time and every music have its unique style and rhythm. As more and more music are released people have the need to access information regarding the latest trends quickly, using reliable methods. This project aims to organize  the information available to better allow an analysis of the evolving trends in music.

Using API of various services like Spotify, Billboard, Genius data is collected. Using the available data sources, an end to end data pipeline is built which is capable to work with big volumes of data.The data lake will consist of logical tables partitioned by certain columns to optimize query latency and can be used to query for improving decision making.

This project is the capstone project for Udacity Data Engineering Nanodegree.The purpose of the capstone project is to combine the skills and expertise acquired during the data engineering nanodegree to accomplish data engineering tasks.

### Technology Stack
1. AWS S3 - For string data lake Amazon S3 is used, which is is an object storage service that offers industry-leading scalability, data availability, security, and performance. S3 is a perfect places for storing our data partitioned and grouped in files. It has low cost and a lot of flexibility.

2. Apache Spark and Amazon EMR - For ETL data processing Apache Spark and Amazon EMR is being used because of the capacity to process large amounts of data. Sprak provides great performance because it stores the data in-memory shared across the cluster.

3. Apache Airflow -  Airflow provides an intuitive UI where we can track the progress and bottlenecks of our pipelines.

### ETL Flow

 - Dataset and processing codes are moved to S3 buckets.
 - For ETL process Spark is used on an EMR cluster. 
 - Spark job is triggered which reads the data from bucket and apply transformation. Dataset is repartitioned and moved to the Processed S3 bucket.
 - To orchestrate everything, data pipelineare built using Apache Airflow.
 
 
 ### Project Structure
```
Music-Trends-ETL--pipeline
│   README.md                                    # Project description
│   requirements.txt                             # Python dependencies
|   emr_default.json                             # EMR cluster configuration
|
|___dataset
|   |
│   └───artists_data                                                      
│   └───charts_data                          
│   └───genre_data                          
│   └───lyrics_data                          
│   └───playlist_data 
|   
|___assets                                      # Screenshot of various required files
│       └───load_raw.JPG                        # Move data and scripts to s3 DAG Graph View
│       └───emr_etl.JPG                         # ETL DAG Graph View
│       └───ERD.JPG                             # ER diagram of data model
│       └───connections.png                     # guide to find the airflow connections
|   
|___src 
|   |
│   └───airflow                                 # Airflow home
│   │    │─── dags                              # DAG directory
│   │    │     └─── load_raw_dag.py             # Move data to s3 DAG
│   │    │     └─── emr_etl_dag.py              # ETL dag
│   │    │─── plugins                           
│   │    │     └─── operators                   # Custom operators
│   │    │     │     └─── create_s3_bucket.py   # CreateS3BucketOperator
│   │    │     │     └─── data_quality.py       # CheckS3FileCountOperator 
│   │    │     │     └─── upload_file_s3.py     # UploadFilesToS3Operator
│   └─── script                                 
│   │    └─── process_artists_datal.py          # Artist data etl script
│   │    └─── process_charts_data.py            # Charts data etl script
│   │    └─── process_genre_data.py             # Genre data etl script
│   │    └─── process_lyrics_data.py            # Lyrics data etl script
│   │    └─── process_playlist_data.py          # Playlist data etl script
│   │    └─── check_data_quality.py             # Data quality check
```


### Data Model
Two data lakes are constructed:
1. Raw Data Lake - The purpose of this data lake is to represent a single source of truth and to store data generated from different sources in raw format. In data lake raw data is retained, as we can always go back to our raw data lake and change our ETL process or easily add new pipelines.

2. Optimized Data Lake - This is what we are using for analytics. The data is prepared, compressed and partitioned by certain  columns to 
    allow for fast query times.

![data model](https://github.com/anupam-b/Music-Trends-ETL-pipeline/blob/main/assets/ERD.jpg)

 Star schema is used which contains a fact table `music_table` and five dimensional tables namely `song_table`, `tracks_table`, `lyrics_table`, `artist_table`, and `features_table`.
 
### Data Pipelines
 
 There are two DAGs in the project:
 1. Load_raw_dag -  This pipeline creates the three S3 buckets,one for our raw data, the second for our data lake and a 3rd bucket for our scripts, and uploads the data from local machine.  
![load_raw_dag](https://github.com/anupam-b/Music-Trends-ETL-pipeline/blob/main/assets/load_raw.JPG)

| Step | Type | Description |
| --- | --- | --- |
| Begin_execution | DummyOperator | Dummy task with no action |
| Create_raw_datalake | CreateS3BucketOperator |  Creates S3 bucket |
| Upload_charts_data,<br/>Upload_playlist_data, <br/>Upload_genre_data, <br/>Upload_lyrics_data, <br/>Upload_rtist_data | UploadFilesToS3Operator | Move data from local to s3 bucket|
| Check_data_quality | CheckS3FileCount | Check wether all the data was uploaded or not |
| Create_code_bucket,<br/> Create_datalake_bucket| CreateS3BucketOperator |  Creates S3 bucket |
| Upload_etl_code | UploadFilesToS3Operator | Upload etl scripts to S3 |
| Stop_execution | DummyOperator | Dummy task with no action |


2. EMR_etl_dag - This pipeline extracts the data from raw data bucket, transforms it with Spark cluster on EMR to create dimension and fact tables and writes it back to S3 in a data lake.
![emr_etl_dag](https://github.com/anupam-b/Music-Trends-ETL-pipeline/blob/main/assets/emr_etl.JPG)

| Step | Type | Description |
| --- | --- | --- |
| Begin_execution | DummyOperator | Dummy task with no action |
| Create_EMR_cluster | EmrCreateJobFlowOperator | Creates an EMR Cluster |
| Add_jobflow_steps | EmrAddStepsOperator | Adds steps to an existing EMR JobFlow|
| genre_processing_step,<br/>palylsit_processing_step,<br/>charts_processing_step,<br/>lyrics_processing_step,<br/>artists_processing_step | EmrStepSensor | Asks for the state of the step until it reaches<br/> a terminal state|
| Stop_execution | DummyOperator | Dummy task with no action |

### Installation
#### Clone the repo from github by running:
```
git clone git@github.com:jonathankamau/udend-capstone-project.git
```
#### Install requirements
```
pip install -r requirements.txt
```
#### Start Ariflow UI
Access localhost:8080 in your browser and login to Airflow.

#### Configure Airflow
-  **Airflow connections**
    
    Navigate to *Admin* >> *Connections* >> *Add a new record*, then enter the following values :
        - Conn Id: Enter aws_credentials.
        - Conn Type: Select Amazon Web Services.
        - Login: Enter your Access key ID from the IAM User credentials.
        - Password: Enter your Secret access key from the IAM User credentials.
        - Extra: Add the default region name. { "region_name": "us-west-2" }

-  **EMR config**
    
    Navigate to *Admin* >> *Connections*, Select the 'emr_default' connection.
    Copy everything from `emr_config.json` and paste it in the field 'Extra' then save.

#### Start raw_datalake DAG
In the navigation bar of the Airflow UI click on 'DAGs', then turn `ON` the `load_raw_dag.py`. Refresh the page and click on the trigger dag button.
This pipeline creates the S3 bucket for our raw data lake and uploads the files from local machine. Wait until the pipeline has successfully completed (it should take around 5-10 minutes). Navigate to Graph View to view the current DAG state. 

#### Start optimized datalake ETL DAG
In the navigation bar of the Airflow UI click on 'DAGs', then turn `ON` the `emr_etl_dag.py`. Refresh the page and click on the trigger dag button.

This pipeline extracts the data from our raw data lake, transforms is using Spark on an EMR cluster and saves it in way that is optimizing our query efficiency. Wait until the pipeline has successfully completed (it should take around 15 minutes).

### Addressing Other Scenarios
**1. The data was increased by 100x**
   
-  With AWS, we could easily scale the existing system by choosing more powerful configuration, or even adding new clusters if needed. Instead of running spark session on few EC2 instances, create a cloud formation stack to spin up EMR cluster to handle the volume. 
- Airflow can be installed on the EMR cluster. Once the pipeline is completed, terminate the cluster for cost optimization.


**2. The pipelines would be run on a daily basis by 7 am every day.**
    
 - The pipeline can be ran daily at 7am by changing the `schedule_interval` argument for the airflow dag. It is currently set for a month interval.


**3. The database needed to be accessed by 100+ people.**
    - Computaion power of the system should be increased, distributed datawarehouse solution like Redshift can be used, in order to for faster query results. AWS-Redshift provides elastic resize for a quick adjustment of performance as well as concurrency scaling, which will improve performance while many users are using the cluster.

