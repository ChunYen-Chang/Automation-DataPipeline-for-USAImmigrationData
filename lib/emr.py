# import packages
import requests, json
import time


# define functions
def create_spark_session(master_dns, kind='spark'):
    """
    Description: This function helps end-user to create a session in Spark cluster (by Apache Livy).
                 end-user can use this session to talk with spark cluster and execute some commands.
    
    Parameters: -master_dns: the dns of spark cluster
                -kind: the language you want to use for commucating with this spark cluster
    
    Returns: response.headers: the header information about the session
    """
    host = 'http://' + master_dns + ':8998'
    data = {'kind': kind,
            "conf" : {"spark.jars.packages" : "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                      "spark.driver.extraJavaOptions" : "-Dlog4jspark.root.logger=WARN,console"
            }
           }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    return response.headers


def wait_for_idle_session(master_dns, response_headers):
    """
    Description: This function helps end-user to check the status of this spark cluster.
                 It will return a value when this spark cluster status is equal to idel.
    
    Parameters: -master_dns:  the dns of this spark cluster
                -response_headers: the return value that "create_spark_session" function returns

    Returns: -session_url: return the session status information
    """
    # wait for the session to be idle
    status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    while status != 'idle':
        time.sleep(1)
        status_response = requests.get(session_url, response_headers)
        status = status_response.json()['state']
    return session_url


def submit_statement(session_url, statement_path, args = ''):
    """
    Description: This function helps end-user to send the statement that he wants to execute
                 by the spark cluster. One thing should be mentioned is that the statement
                 language can be scala or pyspark.

    Parameters: -session_url: the value that "wait_for_idle_session" function returns
                -statement_path: the file path of your statement file
                -args: other statement you want to add

    Returns: -response: the response from this spark cluster
    """
    statements_url = session_url + '/statements'
    with open(statement_path, 'r') as f:
        code = f.read()
    code = args + code
    print(code)
    data = {'code': code}
    print(data)
    response = requests.post(statements_url, data=json.dumps(data),
                             headers={'Content-Type': 'application/json'})
    
    return response


def track_statement_progress(master_dns, response_headers):
    """
    Description: This function helps end-user to track the execution status the spark cluster 
                 returns.

    Parameters: -master_dns: the spark cluster dns
                -response_headers: the value that "submit_statement" functions returns.
                    Example: statemenr_response = submit_statement(......)
                             track_statement_progress(the spark dns, statemenr_response.headers)

    Returns: the log infornation from the spark cluster
    """
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location'].split('/statements', 1)[0]
    
    # Poll the status of the submitted scala code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        print('Statement status: ' + statement_status)
        if 'progress' in statement_response.json():
            print('Progress: ' + str(statement_response.json()['progress']))
        
        time.sleep(10)
    
    final_statement_status = statement_response.json()['output']['status']
    
    if final_statement_status == 'error':
        print('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            print(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    
    # Get the logs
    lines = requests.get(session_url + '/log', 
                        headers={'Content-Type': 'application/json'}).json()['log']
    print('Final Statement Status: ' + final_statement_status)
    return lines


def kill_spark_session(session_url):
    """
    Description: This function helps end-user to terminate the spark session

    Parameters: -session_url: the value which is returned by "wait_for_idle_session" function

    Returns: None
    """
    requests.delete(session_url, headers={'Content-Type': 'application/json'})


def submit_command_to_emr(cluster_dns, **kwargs):
    """
    Description: This function is the combination of above functions. It allows the airflow dag to
                 only call this function (not to call above all functions) 

    Parameters: -cluster_dns: the spark cluster dns
                -**kwargs: the parameter from airflow context

    Returns: -monthandyear: this value will be saved in airflow, and we will need this one when we
                define "insert_table_I94Immigration_form_task" in airflow DAG 
    """
    headers = create_spark_session(cluster_dns, 'pyspark')
    session_url = wait_for_idle_session(cluster_dns, headers)

    execution_date = kwargs["execution_date"]
    month = execution_date.strftime("%b").lower()
    year = execution_date.strftime("%y")
    monthandyear = month+year

    statement_response = submit_statement(session_url, kwargs['params']['file'], "monthyear = '{}'\n".format(month+year))
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)
    return monthandyear
