<p align="center">
  <img width="700" height="380" src="https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/logo.jpg">
</p>

# Automation DataPipeline for USA Immigration Data

### PROJECT BACKGROUND AND SUMMARY

##### *BACKGROUND*
I-94 is a form which is used by U.S. Customs and Border Protection to keep track of the arrival and departure to/from the United States
of people who are not United States citizens or lawful permanent residents (Wikipedia, 2019). This form contains many useful data (such 
as a person's destination, arrival date, arrival flight, the allowed-to-stay period, occupation, and so forth) which can be used by 
U.S government departments or research facilities to conduct further deeper analyses and develop future immigration-relating policy. 
Due to the reason that the immigration policy-making is a hot topic right now, this project aims to collect I-94 form data, transform 
data into a easy-to-use format, and load data into a dataware house which allows government departments and research facilities to
access. Besides, in order to provide much more detailed data for users (government departments and research facilities), this project 
also collects U.S. City Demography Data (this dataset comes from OpenSoft) and Airport Code Table (this dataset details the airport
information) and puts these two tables' data together with I-94 form data. 

#### *PROJECT DESCRIPTION*
The main goal of this project is creating a data warehouse which stores clean, well-organized, and detailed data in flask-schema
for U.S government departments and research facilities. To achieve this goal, this project builds an automation data pipeline. 
This automation data pipeline helps us to extract data, transform data, and load data to the final datawaresouse. The description of
the data pipeline conceptual structure is below.

First, this data pipeline uploads "I-94 form SAS files", "I-94 form description text files", "U.S. City Demography Data csv files",
and "Airport Code Table csv files" to the cloud storage (AWS S3). Then, one EC2 instance accesses to this S3 bucket, extracts "I-94 
form description text files", "U.S. City Demography Data csv files", and "Airport Code Table csv files", transforms these csv files 
by python pandas packages, and loads the result back to S3. At the same time, the "I-94 form SAS files" will be sent to a spark cluster
(AWS EMR), receive data wrangling treatments, and save the after-processing data back to S3. Next, all after-processing data will be 
sent to a data warehouse (AWS Redshift) and use flask-schema to store the data. You can check the picture below. 

<p align="center">
  <img width="800" height="500" src="https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/conceptual_datapipeline.jpg">
</p>

#
#### *DATA PIPELINE DETAILS*
In the previous section, it already mentioned the conceptual structure. Therefore, this section will dig into the conceptual structure
and talk about the details. This project uses Apache airflow to create the automation data pipeline. Airflow allows us to create a data
flow from local to S3, S3 to EC2 and Spark cluster, EC2 and Spark cluster to S3, and S3 to Redshift. Let us see the airflow DAG.

- **DAG for this project**
![](https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/DAG_total.jpg)
From this picture, it shows the whole airflow DAG. However, for the sake of the picture size, it is hard for us to see the details
inside this picture. Thus, we seperate this picture into left part and right part and show these two parts in the following pictures.

- **Left side of this DAG**
![](https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/DAG_left.jpg)
**Explanation :**  
The tasks in this photo can be categorized into three parts. The first part is uploading data from local server to AWS S3 (the 
corresponding task is **upload_data_to_s3**). The second part is about data transformation. In this part, we use two different way
to treat the data. The first way is using python pandas packages to do data wrangling task (the corresponding tasks are 
**read_demography_csv_from_s3**, **read_i94SASLabel_from_s3**, **read_airport_csv_from_s3**, **process_demography_data_bycity**,
**process_demography_data_bystate**, **process_i94SASLabels**, **process_airport_data**). The second way is using spark cluster to
do the data cleaning task (the correspond task is **submit_command_to_emr**). The trird part relates to load after-processing dataset back to S3. (the correspond tasks are **write_demographybycity_data_to_s3**
, **write_demographybystate_ data_to_s3**, **write_i94mode_to_s3**, **write_i94addr_to_s3**, **write_i94port_to_s3**, **write_i94visa_to_s3**,
**write_i94CITandi94RES_to_s3**, and **write_airport_data_to_s3**).

- **Right side of this DAG**
![](https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/DAG_right.jpg)
**Explanation :**  
The tasks in this picture also can be sepetated into three steps. The first step is dropping the dimensional tables in Redshift. The reason
why we only drop the dimensional table is that we don't need to keep the old dimensional table data and we want the data in dimensional data 
should be the latest. Therefore, every time we load dimensional table data into Redshift, we drop the existing dimensional table, create new
dimensional table, and save the latest data inside. The second step is creating tables in Redshift (if the table does not exist). The third
step is about getting data from S3 and inserting into each table.

- **Other settings for this DAG :**  
1.The DAG does not have dependencies on past runs  
2.On failure, the task are retried 3 times  
3.Retries happen every 5 minutes  
4.This dag only run once per month (For each month, staffs will delete the old data files and put the latest data files 
in the local server's folder. After that, the airflow will run automatically to do the following ETL tasks)
#
#### *DATA MODELING*
Data modeling (Flask schema)
![](https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/data_modeling_v3.jpg)
- **Explanation :**  
In this project, we decide to use flask schema to store the data. **I94Immigration_form is the fact table**. **Others are the
dimensional tanbles.** There are two reasons for us to make the decision of choosing
the flask schema. The first reason is query performance. The flask schema has small number of tables and clear join paths. If a 
user want to extract data, he does not need to join many tables to get what he needs (it will take a lot of time). He only needs
to join two or three tables and he can get the data he needs. This features ensure the query performance. The second reason is 
flask schema is easy to understand. Our target audience may not have the knowledge of database. He may suffer from frustration
when he needs to extract data from a database having a complex data modeling design. Therefore, the flask schema provides a solution
for him. It is easy to understand and he might quickly get the idea how to extract the data he needs.  
#
#### *DATASETS*
This project contains four datasets.  

- **I94 immigration SAS Data**: This data comes from the US national tourism and Trade office. It contains a person's arrival flight,
destination, occupation, and so forth. The data format is "sas7bdat", which is a SAS data format. The data file size is big--around 600mb.
each file contains around five million records. Due to the size issue, it is hard for a standalone server to do the data wrangling job.
Therefore, this project decides to use Spark cluster to deal with this issue. The Spark cluster can handle this kind of data and finish the
data wrangling task.  

- **I94 immigration description text Data**: This data also comes from the US national tourism and Trade office. It contains the metadata
about I94 immigration SAS Data. Since this data file size is not big, we do not consider to use Spark cluster. We only use the standalone 
server which has python to do the data wrangling job. The python program will extract the text information from this data and save this data in 
a format this project needs.  

- **US city demography data**: This data comes from OpenSoft. It contains the demography data about each US city. Since this file size is not
big, we also use the standalone server to do the data wrangling job. The python program will extract the necessary columns, fill the null value, and
save data in a format this project wants.  

- **Airport Code Table**: This is a dataset which contains airport codes and corresponding information such as city's information and airport's GPS location.
Because the data is not big, this project will use python to do the data wrangling job.



------------
#### FILES IN THE REPOSITORY
- **image folder**: It contains images which are used in README.md file  

- **lib folder**: It contains five .py files.  
    1. **emr.py**: this file includes the python functions which help user to submit a script to Spark cluster.
    2. **immigration_pyspark.py**: this file includes the script which will be sent to Spark cluster.
    3. **python_function.py**: this file includes the python functions which relates to extract data from S3, transform data, and load data back to S3
    4. **redshift_function.py**: this file includes the python function about dropping Redshift table, creating Redshift table, and copy data from S3 to Redshift table.
    5. **redshift_sql_syntax.py**: a python script which defines the SQL command that will be used in this project  

- **i94immigration.py**: a python script which is used for defining the DAG

------------
#### HOW TO RUN THE PROJECT
**Please follow below steps :**
1. Have AWS_key_id, AWS_secret_key, redshift dns, redshift username, redshift password, EMR dns  

2. Have a local server running ubuntu 16.04 OS  

3. Install Docker in your local server: type `apt install docker.io` in your local server's terminal  

4. Install Docker Compose: type `curl -L "https://github.com/docker/compose/releases/download/1.10.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose` in your terminal  

5. Give execution right to docker-compose: type `chmod +x /usr/local/bin/docker-compose`  

6. Clone this repo: https://github.com/puckel/docker-airflow to your terminal  

7. cd to ~/airflow-tutorial/examples/intro-example/dags  

8. Remove all files in this folder  

9. Copy this project's files to ~/airflow-tutorial/examples/intro-example/dag  

10. cd to ~/airflow-tutorial  

11. Type `sudo docker-compose up -d` in your terminal to start the airflow  

12. Use your web browser to access to local:8088  

13. Clink the on button  
![](https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/airflow_demo1.jpg)  

14. Monitor your datapipeline
![](https://github.com/ChunYen-Chang/Automation-DataPipeline-for-USAImmigrationData/blob/master/image/airflow_demo2.jpg)


