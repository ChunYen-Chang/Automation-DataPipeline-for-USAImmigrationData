#-----------------------------------------------------------------------------------#
# import packages                                                                   #
#-----------------------------------------------------------------------------------#
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator
import lib.emr as libemr
import lib.python_function as pyfun
import lib.redshift_sql_syntax as red_sql
import lib.redshift_function as red_fun


#-----------------------------------------------------------------------------------#
# define connection parameters for AWS Redshift, AWS EMR, and AWS S3                #
#-----------------------------------------------------------------------------------#
# Redshift parameters
redshift_host_dns = 'YOUR REDSHIFT HOST DNS'
redshift_database = 'YOUR REDSHIFT DATABASE NAME'
redshift_username = 'REDSHIFT USERNAME'
redshift_password_for_red = 'REDSHIFT PASSWORD'
redshift_port_num = 'REDSHIFT PORT NUMBER'


# S3 parameters
key_id = 'YOUR AWS ACCESS KEY ID'
key_secret = 'YOUR AWS SECRET KEY'


# EMR parameters
emr_host_dns = 'YOUR EMR CLUSTER DNS'


#-----------------------------------------------------------------------------------#
# define DAG arguments and create a DAG                                             #
#-----------------------------------------------------------------------------------#
default_args = {
    'owner': 'ChunYen-Chang',
    'start_date': datetime(2019, 9, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}


dag = DAG('i94form_analysis_dag',
          default_args=default_args,
          description='Extract i94form data from S3, transform it by python and spark, load it back to S3, and transfer it to Redshift',
          schedule_interval='0 0 1 * *',
          catchup=False,
          max_active_runs=1
        )


#-----------------------------------------------------------------------------------#
# define task in this DAG                                                           #
#-----------------------------------------------------------------------------------#
#   the dag structure explanation:                                                  #
#       This dag can be seperated into two parts. The first part is extracting data #
#       from S3, transforming data, and saving data back to S3. The second part is  #
#       moving the after-processing data from S3 to Redshift.                       #
#                                                                                   #
#       In the first part,due to the data file size, we decide to use two different #
#       ways to transform the data. One is using the python panda package (for the  #
#       small data file size.) Another one is using spark cluster. (for the data    #
#       file which has big size). Thus, in the belowing section, you can see we     #
#       design two different data pipelines. One extracts data from s3 to EC2       #
#       instance, transforms data, and saves result back to S3. Another one extracts# 
#       data from s3 to a spark cluster, transform data, and saves result back to S3#
#                                                                                   #
#       In the second part, since this project uses the flask schema in data        #
#       warehouse, we define I94immigration_form as fact table, other tables as     #
#       dimensional table. In the beginning of second part, we drop tables, create  #
#       tables, and copy data fro S3 to Redshift                                    #
#-----------------------------------------------------------------------------------#
# define start_operator task
start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)


# part 1: upload data to S3 (from local to S3)
upload_data_to_s3_task = PythonOperator(
    task_id='upload_data_to_s3',
    python_callable=pyfun.upload_to_aws,
    op_kwargs={
        'bucket': 'udacityfikrusnk',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    dag=dag)


# part 1: extract data process (from s3 to EC2 instance)
read_airport_csv_from_s3_task = PythonOperator(
    task_id='read_airport_csv_from_s3',
    python_callable=pyfun.read_csv_from_s3,
    op_kwargs={        
        'bucket': 'udacityfikrusnk',
        's3_file': 'airport-codes_csv.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    dag=dag)


read_demography_csv_from_s3_task = PythonOperator(
    task_id='read_demography_csv_from_s3',
    python_callable=pyfun.read_csv_from_s3,
    op_kwargs={
        'bucket': 'udacityfikrusnk',
        's3_file': 'us-cities-demographics.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret,
        'delimiterValue': ';'
    },
    dag=dag)


read_I94SASLabels_from_s3_task = PythonOperator(
    task_id='read_I94SASLabels_from_s3',
    python_callable=pyfun.read_txt_from_s3,
    op_kwargs={
        'bucket': 'udacityfikrusnk',
        's3_file': 'I94_SAS_Labels_Descriptions.SAS',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    dag=dag)


# part1: data transformation process (by python package)
process_airport_data_task = PythonOperator(
    task_id='process_airport_data',
    python_callable=pyfun.process_airport_data,
    provide_context=True,
    dag=dag)


process_demography_data_bycity_task = PythonOperator(
    task_id='process_demography_data_bycity',
    python_callable=pyfun.process_demography_data_bycity,
    provide_context=True,
    dag=dag)


process_demography_data_bystate_task = PythonOperator(
    task_id='process_demography_data_bystate',
    python_callable=pyfun.process_demography_data_bystate,
    provide_context=True,
    dag=dag)


process_I94SASLabels_task = PythonOperator(
    task_id='process_I94SASLabels',
    python_callable=pyfun.process_I94SASLabels,
    provide_context=True,
    dag=dag)


# part1: data loading process (from EC2 instance to S3)
write_airport_data_to_s3_task = PythonOperator(
    task_id='write_airport_data_to_s3',
    python_callable=pyfun.write_airport_data_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk', 
        'filename': 'airport-codes-after-process.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_demographybycity_data_to_s3_task = PythonOperator(
    task_id='write_demographybycity_data_to_s3',
    python_callable=pyfun.write_demographybycity_data_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'us-demographics-by-city.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_demographybystate_data_to_s3_task = PythonOperator(
    task_id='write_demographybystate_data_to_s3',
    python_callable=pyfun.write_demographybystate_data_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'us-demographics-by-state.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_I94CITandI94RES_to_s3_task = PythonOperator(
    task_id='write_I94CITandI94RES_to_s3',
    python_callable=pyfun.write_I94CITandI94RES_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'I94CITandI94RES.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_I94PORT_to_s3_task = PythonOperator(
    task_id='write_I94PORT_to_s3',
    python_callable=pyfun.write_I94PORT_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'I94PORT.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_I94MODE_to_s3_task = PythonOperator(
    task_id='write_I94MODE_to_s3',
    python_callable=pyfun.write_I94MODE_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'I94MODE.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_I94ADDR_to_s3_task = PythonOperator(
    task_id='write_I94ADDR_to_s3',
    python_callable=pyfun.write_I94ADDR_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'I94ADDR.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


write_I94VISA_to_s3_task = PythonOperator(
    task_id='write_I94VISA_to_s3',
    python_callable=pyfun.write_I94VISA_to_s3,
    op_kwargs={
        'bucketname': 'udacityfikrusnk',
        'filename': 'I94VISA.csv',
        'AWS_ACCESS_KEY_ID': key_id,
        'AWS_SECRET_ACCESS_KEY': key_secret
    },
    provide_context=True,
    dag=dag)


# part1: extract data form S3, transform data by spark, and loadk data back to S3
submit_command_to_emr_task = PythonOperator(
    task_id='submit_command_to_emr',
    python_callable=libemr.submit_command_to_emr,
    op_kwargs={
        'cluster_dns': emr_host_dns
    },
    params = {"file" : '/usr/local/airflow/dags/lib/immigration_pyspark.py'},
    provide_context=True,
    dag=dag)


# the middlw execution phase
middle_operator = DummyOperator(task_id='middle_execution',  dag=dag)


# part2: drop tables
drop_table_I94CIT_I94RES_Code_task = PythonOperator(
    task_id='drop_table_I94CIT_I94RES_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94CIT_I94RES_Code_drop
    },
    dag=dag)


drop_table_I94Addr_Code_task = PythonOperator(
    task_id='drop_table_I94Addr_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Addr_Code_drop
    },
    dag=dag)


drop_table_US_Demography_by_State_task = PythonOperator(
    task_id='drop_table_US_Demography_by_State',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.US_Demography_by_State_drop
    },
    dag=dag)


drop_table_I94Port_Code_task = PythonOperator(
    task_id='drop_table_I94Port_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Port_Code_drop
    },
    dag=dag)


drop_table_I94Mode_Code_task = PythonOperator(
    task_id='drop_table_I94Mode_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Mode_Code_drop
    },
    dag=dag)


drop_table_I94Visa_Code_task = PythonOperator(
    task_id='drop_table_I94Visa_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Visa_Code_drop
    },
    dag=dag)


drop_table_US_Demography_by_City_task = PythonOperator(
    task_id='drop_table_US_Demography_by_City',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.US_Demography_by_City_drop
    },
    dag=dag)


drop_table_Airport_Information_task = PythonOperator(
    task_id='drop_table_Airport_Information',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.Airport_Information_drop
    },
    dag=dag)


# part2: create tables
create_table_I94Immigration_form_task = PythonOperator(
    task_id='create_table_I94Immigration_form',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Immigration_form_table_create
    },
    dag=dag)


create_table_I94CIT_I94RES_Code_task = PythonOperator(
    task_id='create_table_I94CIT_I94RES_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94CIT_I94RES_table_create
    },
    dag=dag)


create_table_I94Addr_Code_task = PythonOperator(
    task_id='create_table_I94Addr_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Addr_Code_table_create
    },
    dag=dag)


create_table_US_Demography_by_State_task = PythonOperator(
    task_id='create_table_US_Demography_by_State',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.US_Demography_by_State_table_create
    },
    dag=dag)


create_table_I94Port_Code_task = PythonOperator(
    task_id='create_table_I94Port_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Port_Code_table_create
    },
    dag=dag)


create_table_I94Mode_Code_task = PythonOperator(
    task_id='create_table_I94Mode_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Mode_Code_table_create
    },
    dag=dag)


create_table_I94Visa_Code_task = PythonOperator(
    task_id='create_table_I94Visa_Code',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Visa_Code_table_create
    },
    dag=dag)


create_table_US_Demography_by_City_task = PythonOperator(
    task_id='create_table_US_Demography_by_City',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.US_Demography_by_City_table_create
    },
    dag=dag)


create_table_Airport_Information_task = PythonOperator(
    task_id='create_table_Airport_Information',
    python_callable=red_fun.postgres_dropandcreate,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.Airport_Information_table_create
    },
    dag=dag)


# copt data from S3 to each table
insert_table_I94Addr_Code_task = PythonOperator(
    task_id='insert_table_I94Addr_Code',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Addr_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_I94CIT_I94RES_Code_task = PythonOperator(
    task_id='insert_table_I94CIT_I94RES_Code',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94CITandI94RES_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_I94Mode_Code_task = PythonOperator(
    task_id='insert_table_I94Mode_Code',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Mode_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_I94Port_Code_task = PythonOperator(
    task_id='insert_table_I94Port_Code',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Port_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_I94Visa_Code_task = PythonOperator(
    task_id='insert_table_I94Visa_Code',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Visa_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_Airport_Information_task = PythonOperator(
    task_id='insert_table_Airport_Information',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.Airport_Information_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_US_Demography_by_City_task = PythonOperator(
    task_id='insert_table_US_Demography_by_City',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.US_Demography_by_City_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_US_Demography_by_State_task = PythonOperator(
    task_id='insert_table_US_Demography_by_State',
    python_callable=red_fun.postgres_insert,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.US_Demography_by_State_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
    },
    dag=dag)


insert_table_I94Immigration_form_task = PythonOperator(
    task_id='insert_table_I94Immigration_form',
    python_callable=red_fun.postgres_insert_immigration,
    op_kwargs={
        'host_dns': redshift_host_dns,
        'database': redshift_database,
        'username': redshift_username,
        'password_redshift': redshift_password_for_red,
        'port_num': redshift_port_num,
        'syntax': red_sql.I94Immigration_form_table_insert,
        'aws_key_id': key_id,
        'aws_secret_key': key_secret
        },
    provide_context=True,
    dag=dag)


# define end_operator task
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#-----------------------------------------------------------------------------------#
# define the data flow                                                              #
#-----------------------------------------------------------------------------------#
start_operator >> upload_data_to_s3_task
upload_data_to_s3_task >> submit_command_to_emr_task >> middle_operator
upload_data_to_s3_task >> [read_airport_csv_from_s3_task, read_demography_csv_from_s3_task, read_I94SASLabels_from_s3_task]
read_airport_csv_from_s3_task >> process_airport_data_task >> write_airport_data_to_s3_task
read_demography_csv_from_s3_task >> process_demography_data_bycity_task >> [write_demographybycity_data_to_s3_task, process_demography_data_bystate_task]
process_demography_data_bystate_task >> write_demographybystate_data_to_s3_task
read_I94SASLabels_from_s3_task >> process_I94SASLabels_task >> [write_I94CITandI94RES_to_s3_task, write_I94PORT_to_s3_task, write_I94MODE_to_s3_task, write_I94ADDR_to_s3_task, write_I94VISA_to_s3_task]
[write_airport_data_to_s3_task, write_demographybycity_data_to_s3_task, write_demographybystate_data_to_s3_task] >> middle_operator
[write_I94CITandI94RES_to_s3_task, write_I94PORT_to_s3_task, write_I94MODE_to_s3_task, write_I94ADDR_to_s3_task, write_I94VISA_to_s3_task] >> middle_operator
middle_operator >> drop_table_I94CIT_I94RES_Code_task >> create_table_I94CIT_I94RES_Code_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_I94Addr_Code_task >> create_table_I94Addr_Code_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_US_Demography_by_State_task >> create_table_US_Demography_by_State_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_I94Port_Code_task >> create_table_I94Port_Code_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_I94Mode_Code_task >> create_table_I94Mode_Code_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_I94Visa_Code_task >> create_table_I94Visa_Code_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_US_Demography_by_City_task >> create_table_US_Demography_by_City_task >> create_table_I94Immigration_form_task
middle_operator >> drop_table_Airport_Information_task >> create_table_Airport_Information_task >> create_table_I94Immigration_form_task
create_table_I94Immigration_form_task >> insert_table_I94CIT_I94RES_Code_task >> end_operator 
create_table_I94Immigration_form_task >> insert_table_I94Addr_Code_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_US_Demography_by_State_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_I94Port_Code_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_I94Mode_Code_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_I94Visa_Code_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_US_Demography_by_City_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_Airport_Information_task >> end_operator
create_table_I94Immigration_form_task >> insert_table_I94Immigration_form_task >> end_operator
