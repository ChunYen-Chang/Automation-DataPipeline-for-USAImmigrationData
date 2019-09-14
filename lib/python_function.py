# import packages
import boto3
import pandas as pd
from io import StringIO


# define fuctions
def upload_to_aws(bucket, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    """
    Description: This functions helps users to upload files from local server to cloud
                storage (AWS S3)

    Parameters: -bucket: the S3 bucket you want to put files in
                -AWS_ACCESS_KEY_ID: the id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the password for connecting to S3

    Returns: None
    """
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    s3.upload_file('I94_SAS_Labels_Descriptions.SAS', bucket, 'I94_SAS_Labels_Descriptions.SAS')
    s3.upload_file('airport-codes_csv.csv', bucket, 'airport-codes_csv.csv ')
    s3.upload_file('us-cities-demographics.csv', bucket, 'us-cities-demographics.csv')
    s3.upload_file('i94_sep19_sub.sas7bdat', bucket, 'i94_sep19_sub.sas7bdat')


def read_csv_from_s3(bucket, s3_file, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, delimiterValue=None):
    """
    Description: This functions helps users to read S3 files(CSV) and put the data in these files 
                 into python's dataframe

    Parameters: -bucket: the S3 bucket you want to put files in
                -s3_file: the files name in your S3 bucket
                -AWS_ACCESS_KEY_ID: the id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the password for connecting to S3
                -delimiterValue: the delimiter for CSV files

    Returns: dataframe
    """
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    obj = s3.get_object(Bucket=bucket, Key=s3_file)

    if delimiterValue != None:
        df = pd.read_csv(obj['Body'], delimiter=delimiterValue)
    elif delimiterValue == None:
        df = pd.read_csv(obj['Body'])
    return df


def read_txt_from_s3(bucket, s3_file, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    """
    Description: This functions helps users to read S3 files(txt) and put the data in these files
                 into python's dataframe

    Parameters: -bucket: the S3 bucket you want to put files in
                -s3_file: the files name in your S3 bucket
                -AWS_ACCESS_KEY_ID: the id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the password for connecting to S3

    Returns: text
    """
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    obj = s3.get_object(Bucket=bucket, Key=s3_file)

    text = obj['Body'].read().decode("utf-8") .splitlines()

    return text


def process_airport_data(**kwargs):
    """
    Description: This function helps user to process the airport data

    Parameters: airflow context variables

    Returns: after-processing dataframe
    """
    # get data from xcom
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='read_airport_csv_from_s3')
    dataframe = dataframe.fillna('None')
    return dataframe


def process_demography_data_bycity(**kwargs):
    """
    Description: This function helps user to process the demography data

    Parameters: airflow context variables

    Returns: after-processing dataframe
    """
    # get data from xcom
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='read_demography_csv_from_s3')

    # group data by city column
    df_demography_groupby_city = dataframe.groupby('City')

    # calculate for each column
    series_State = df_demography_groupby_city['State'].apply(set).apply(lambda x: ''.join(x).upper())
    series_MedianAge = df_demography_groupby_city['Median Age'].mean()
    series_Malepopulation = df_demography_groupby_city['Male Population'].mean()
    series_FemalePopulation = df_demography_groupby_city['Female Population'].mean()
    series_TotalPopulation = df_demography_groupby_city['Total Population'].mean()
    series_NumberofVeterans = df_demography_groupby_city['Number of Veterans'].mean()
    series_Foreignborn = df_demography_groupby_city['Foreign-born'].mean()
    series_AverageHouseholdSize = df_demography_groupby_city['Average Household Size'].mean()
    series_Count = df_demography_groupby_city['Count'].sum()
    series_Race = df_demography_groupby_city['Race'].apply(set).apply(lambda x: ' / '.join(x))

    # concate all above series and form a data frame
    df_demography_groupby_city = pd.concat([series_State, series_MedianAge, series_Malepopulation, series_FemalePopulation,
                                              series_TotalPopulation, series_NumberofVeterans, series_Foreignborn,
                                              series_AverageHouseholdSize, series_Count, series_Race], axis=1)

    # reset the index for df_demography_groupby_city
    df_demography_groupby_city = df_demography_groupby_city.reset_index()

    # convert value in State column from lowercase to uppercase, this is needed for furture join in Redshift table
    df_demography_groupby_city['City'] = df_demography_groupby_city['City'].apply(lambda x: x.upper())

    return df_demography_groupby_city


def process_demography_data_bystate(**kwargs):
    """
    Description: This function helps user to process the demography_data_bycity data

    Parameters: airflow context variable(though xcom)

    Returns: dataframe
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_demography_data_bycity')

    # group data by State column
    df_demography_groupby_state = dataframe.groupby('State')

    # calculate the mean, sum, and text combination
    series_MedianAge = df_demography_groupby_state['Median Age'].mean()
    series_Malepopulation = df_demography_groupby_state['Male Population'].sum()
    series_FemalePopulation = df_demography_groupby_state['Female Population'].sum()
    series_TotalPopulation = df_demography_groupby_state['Total Population'].sum()
    series_NumberofVeterans = df_demography_groupby_state['Number of Veterans'].sum()
    series_Foreignborn = df_demography_groupby_state['Foreign-born'].sum()
    series_AverageHouseholdSize = df_demography_groupby_state['Average Household Size'].mean()
    series_Count = df_demography_groupby_state['Count'].sum()
    series_Race = df_demography_groupby_state['Race'].apply(set).apply(lambda x: set(''.join(x).split(' / '))).apply(lambda x: ' / '.join(x))

    # concate all above series and form a data frame
    df_demography_groupby_state = pd.concat([series_MedianAge, series_Malepopulation, series_FemalePopulation,
                                              series_TotalPopulation, series_NumberofVeterans, series_Foreignborn,
                                              series_AverageHouseholdSize, series_Count, series_Race], axis=1)

    # reset the index for df_city_demography_groupby_state
    df_demography_groupby_state = df_demography_groupby_state.reset_index()

    return df_demography_groupby_state


def process_I94SASLabels(**kwargs):
    """
    Description: This function helps users to process the I94SASLabels text

    Parameters: airflow context variable (though xcom)

    Returns: None. The after-process is sending out by xcom_push
    """
    ti = kwargs['ti']
    sas_text = ti.xcom_pull(task_ids='read_I94SASLabels_from_s3')

    sas_inf_dict = {}
    temp = []

    # read sas_text and save content into a dictionary(sas_inf_dict)
    for i in sas_text:
        if '/*' in i and '-' in i:
            k, v = i.split(' - ')[0].replace('/* ', ''), \
                        i.split(' - ')[1].replace(' */', '')
            sas_inf_dict[k] = {'inf': v}

        elif '=' in i and ';' not in i:
            data1, data2 = i.split('=')[0].lstrip().rstrip().replace('\'', '').upper(), \
                                i.split('=')[1].lstrip().rstrip().replace('\'', '').upper()
            temp.append([data1, data2])

        elif len(temp) > 1:
            sas_inf_dict[k]['data'] = temp
            temp = []

    # fix values in sas_inf_dict (key = I94PORT)
    temp_for_fix_I94PORT = []
    for i in sas_inf_dict['I94PORT']['data']:
        if len(i[1].split(',')) == 2:
            k, v, r = i[0], i[1].split(',')[0].rstrip().lstrip(), i[1].split(',')[1].rstrip().lstrip()
            temp_for_fix_I94PORT.append([k, v, r])
        elif len(i[1].split(',')) == 1:
            k, v = i[0], i[1].rstrip().lstrip()
            temp_for_fix_I94PORT.append([k, v])

    sas_inf_dict['I94PORT']['data'] = temp_for_fix_I94PORT

    # convert sas_inf_dict into dataframes
    df_I94CITandI94RES = pd.DataFrame(sas_inf_dict['I94CIT & I94RES']['data'], columns=['I94CIT_I94RES_Code', 'Country'])
    df_I94PORT = pd.DataFrame(sas_inf_dict['I94PORT']['data'], columns=['I94port_Code', 'City', 'State'])
    df_I94MODE = pd.DataFrame(sas_inf_dict['I94MODE']['data'], columns=['I94mode_Code', 'Transportation'])
    df_I94ADDR = pd.DataFrame(sas_inf_dict['I94ADDR']['data'], columns=['I94addr_Code', 'State'])
    df_I94VISA = pd.DataFrame(sas_inf_dict['I94VISA']['data'], columns=['I94visa_Code', 'VisaType'])

    # use xcom_push to give these dataframes to write function
    kwargs['ti'].xcom_push(key='df_I94CITandI94RES', value=df_I94CITandI94RES)
    kwargs['ti'].xcom_push(key='df_I94PORT', value=df_I94PORT)
    kwargs['ti'].xcom_push(key='df_I94MODE', value=df_I94MODE)
    kwargs['ti'].xcom_push(key='df_I94ADDR', value=df_I94ADDR)
    kwargs['ti'].xcom_push(key='df_I94VISA', value=df_I94VISA)


def write_airport_data_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID , AWS_SECRET_ACCESS_KEY,**kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name 
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_airport_data')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_demographybycity_data_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_demography_data_bycity')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_demographybystate_data_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_demography_data_bystate')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_I94CITandI94RES_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_I94SASLabels', key='df_I94CITandI94RES')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_I94PORT_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_I94SASLabels', key='df_I94PORT')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_I94MODE_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_I94SASLabels', key='df_I94MODE')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_I94ADDR_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_I94SASLabels', key='df_I94ADDR')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))


def write_I94VISA_to_s3(bucketname, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, **kwargs):
    """
    Description: This function helps users to save after-processing dataframe back to S3

    Parameters: -bucketname: the S3 bucket name
                -filename: the file name
                -AWS_ACCESS_KEY_ID: the aws id for connecting to S3
                -AWS_SECRET_ACCESS_KEY: the aws password for connecting to S3
                -**kwargs: the airflow context variable

    Returns: None
    """
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='process_I94SASLabels', key='df_I94VISA')
    # create a buffer
    csv_buffer = StringIO()
    # write dataframe to this buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # build a connection
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # write data to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)
    print('Write file-{} to AWS S3 bucket {} is done'.format(filename, bucketname))

