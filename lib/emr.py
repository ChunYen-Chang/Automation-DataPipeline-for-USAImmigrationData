#import boto3
import requests, json
import time

def create_spark_session(master_dns, kind='spark'):
    host = 'http://' + master_dns + ':8998'
    data = {'kind': kind,
            "conf" : {"spark.jars.packages" : "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                      "spark.driver.extraJavaOptions" : "-Dlog4jspark.root.logger=WARN,console"
            }
           }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    #logging.info(response.json())
    return response.headers


def wait_for_idle_session(master_dns, response_headers):
    # wait for the session to be idle or ready for job submission
    status = ''
    host = 'http://' + master_dns + ':8998'
    #logging.info(response_headers)
    session_url = host + response_headers['location']
    while status != 'idle':
        time.sleep(1)
        status_response = requests.get(session_url, response_headers)
        status = status_response.json()['state']
        #logging.info('Session status: ' + status)
    return session_url


def submit_statement(session_url, statement_path, args = ''):
    statements_url = session_url + '/statements'
    with open(statement_path, 'r') as f:
        code = f.read()
    code = args + code
    print(code)
    data = {'code': code}
    print(data)
    response = requests.post(statements_url, data=json.dumps(data),
                             headers={'Content-Type': 'application/json'})
    
    # logging.info(response.json())
    return response


def track_statement_progress(master_dns, response_headers):
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
        #logging.info('Statement status: ' + statement_status)
        if 'progress' in statement_response.json():
            print('Progress: ' + str(statement_response.json()['progress']))
            #logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
        print('Statement exception: ' + statement_response.json()['output']['evalue'])
        #logging.info('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            print(trace)
            #logging.info(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    
    # Get the logs
    lines = requests.get(session_url + '/log', 
                        headers={'Content-Type': 'application/json'}).json()['log']
    print('Final Statement Status: ' + final_statement_status)
    #logging.info('Final Statement Status: ' + final_statement_status)
    return lines


def kill_spark_session(session_url):
    requests.delete(session_url, headers={'Content-Type': 'application/json'})


# define functions
def submit_command_to_emr(cluster_dns, **kwargs):
    headers = create_spark_session(cluster_dns, 'pyspark')
    session_url = wait_for_idle_session(cluster_dns, headers)

    # Get execution date format
    execution_date = kwargs["execution_date"]
    month = execution_date.strftime("%b").lower()
    year = execution_date.strftime("%y")
    monthandyear = month+year

    statement_response = submit_statement(session_url, kwargs['params']['file'], "monthyear = '{}'\n".format(month+year))
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)
    return monthandyear
