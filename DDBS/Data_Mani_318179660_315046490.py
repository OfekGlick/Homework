import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from time import sleep
from pyspark.sql.types import *


def init_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


# environment data
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
                             "com.microsoft.azure:spark-mssql-connector:1.0.2 pyspark-shell"
# KAFKA DATA
spark, sc = init_spark('ofek')
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'

# SQL SERVER DATA
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "ofek0glick"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "Project2"
username = "ofek0glick"
password = "Qwerty12!"

# Define the schema of the data:
noaa_schema = StructType([StructField('StationId', StringType(), False),
                          StructField('Date', StringType(), False),
                          StructField('Variable', StringType(), False),
                          StructField('Value', IntegerType(), False),
                          StructField('M_Flag', StringType(), True),
                          StructField('Q_Flag', StringType(), True),
                          StructField('S_Flag', StringType(), True),
                          StructField('ObsTime', StringType(), True)])

string_value_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "IS") \
    .option("rowsPerSecond", 1000) \
    .option("startingOffsets", "earliest") \
    .load().selectExpr("CAST(value AS STRING)")
#
json_df = string_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
streaming_df = json_df.select("json.*")


def handle_batch(batch_df, batch_id):
    print(batch_id)
    batch_df = batch_df.groupby("StationId", "Date").pivot("Variable") \
        .agg(F.first("Value"))
    batch_df = batch_df.withColumn("Date",
                                   F.concat(F.expr("substring(Date,0,4)"), F.lit("-"), F.expr("substring(Date,5,2)"),
                                            F.lit("-"),
                                            F.expr("substring(Date,7,2)"))) \
        .withColumn("Date", F.to_timestamp(F.expr("substring(Date,0,10)"), "yyyy-MM-dd"))
    batch_df.show(20, False)
    # Tried append and overwrite
    try:
        batch_df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .save()
    except ValueError as error:
        print("Connector write failed", error)


query = streaming_df \
    .writeStream \
    .trigger(processingTime='2 seconds') \
    .foreachBatch(handle_batch) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
print("done")
# .outputMode("append") \
# .format("memory") \
# .queryName("kafka_stream") \

# sleep(10)
# query.stop()

# com.microsoft.sqlserver.jdbc.spark
