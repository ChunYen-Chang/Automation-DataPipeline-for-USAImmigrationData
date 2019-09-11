# import packages
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType 

# define functions and register functions to Spark environment by udf
def convert_sastime_to_datetime(x):
	try:
		sas_start_time = datetime(1960, 1, 1)
		correct_time = sas_start_time + timedelta(days=int(x))
		return str(correct_time)
	except:
		return None		

udf_convert_sastime_to_datetime = udf(lambda x: convert_sastime_to_datetime(x), StringType())

def convert_strtime_to_datetime(x):
	try:
		correct_time = datetime.strptime(x, '%m%d%Y')
		return str(correct_time)
		
	except:
		return None
		
udf_convert_strtime_to_datetime = udf(lambda x: convert_strtime_to_datetime(x), StringType())


# start data processing steps
# read data from AWS s3 to Spark
i94_df = spark.read.format('com.github.saurfang.sas.spark').load('s3://udacityfikrusnk/i94_{}_sub.sas7bdat'.format(monthyear))				 		
#i94_df = spark.read.format('com.github.saurfang.sas.spark').load('s3://udacityfikrusnk/i94_jan19_sub.sas7bdat')				 		



# create new columns by using udf functions
i94_df = i94_df.withColumn('arrival_date', udf_convert_sastime_to_datetime('arrdate')).withColumn('departure_date', udf_convert_sastime_to_datetime('depdate')).withColumn('allowedtostay_date', udf_convert_strtime_to_datetime('dtaddto'))

# select necessary columns
i94_df = i94_df.select(['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'i94mode', 'i94addr', 'i94bir', 'i94visa', 'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag' ,'biryear', 'gender', 'airline', 'admnum', 'fltno', 'visatype', 'arrival_date', 'departure_date', 'allowedtostay_date'])

# save the dataframe to S3 in Parquet format
#i94_df.write.mode('append').parquet('s3://udacityfikrusnk/i94_immigration_after/{}'.format(monthyear))
#i94_df.write.format("com.databricks.spark.csv").save('s3://udacityfikrusnk/i94_immigration_after/jan19')
i94_df.write.format("com.databricks.spark.csv").save('s3://udacityfikrusnk/i94_immigration_after/{}'.format(monthyear))
