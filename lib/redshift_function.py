# import packages
import psycopg2

# define functions
def postgres_dropandcreate(host_dns, database, username, password_redshift, port_num, syntax):
    """
    Description: This function helps users to drop Redshift tables.

    Parameters: -host_dns: Redshift DNS
                -database: Redshift database name
                -username: Redshift username
                -password_redshift: Redshift password
                -port_num: Redshift port
                -syntax: The SQL code for deleting Redshift tables

    Returns: None
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host_dns, database, username, password_redshift, port_num))
    cur = conn.cursor()
    cur.execute(syntax)
    conn.commit()


def postgres_insert(host_dns, database, username, password_redshift, port_num, syntax, aws_key_id, aws_secret_key):
    """
    Description: This function helps users to copy data from S3 to Redshift dimensional tables

    Parameters: -host_dns: Redshift DNS
                -database: Redshift database name
                -username: Redshift username
                -password_redshift: Redshift password
                -port_num: Redshift port
                -syntax: The SQL code for loading data into Redshift dimensional tables
                -aws_key_id: the aws id for connecting to S3
                -aws_secret_key: the aws password for connecting to S3

    Returns: None
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host_dns, database, username, password_redshift, port_num))
    cur = conn.cursor()
    print(syntax)
    syntax = syntax.format(aws_key_id,aws_secret_key)
    print(syntax)
    cur.execute(syntax)
    conn.commit()


def postgres_insert_immigration(host_dns, database, username, password_redshift, port_num, syntax, aws_key_id, aws_secret_key, **kwargs):
    """
    Description: This function helps users to copy data from S3 to Redshift fact tables

    Parameters: -host_dns: Redshift DNS
                -database: Redshift database name
                -username: Redshift username
                -password_redshift: Redshift password
                -port_num: Redshift port
                -syntax: The SQL code for loading data into Redshift dimensional tables
                -aws_key_id: the aws id for connecting to S3
                -aws_secret_key: the aws password for connecting to S3

    Returns: None
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host_dns, database, username, password_redshift, port_num))
    cur = conn.cursor()
    
    # receive the airflow context varable
    ti = kwargs['ti']

    # extract monthyear variable from xcom
    monthyear = ti.xcom_pull(task_ids='submit_command_to_emr')
    
    syntax = syntax.format(monthyear, aws_key_id,aws_secret_key)
    
    cur.execute(syntax)
    conn.commit()

