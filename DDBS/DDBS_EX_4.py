import findspark

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


spark, sc = init_spark('demo')
data_file = 'globalterrorismdb.csv'
terror_rdd = sc.textFile(data_file)

csv_rdd = terror_rdd.map(lambda row: row.split(','))
header = csv_rdd.first()
data_rdd = csv_rdd.filter(lambda row: row != header).filter(lambda row: 1983 <= int(row[0]) <= 2013).map(
    lambda row: (row[7], 1)).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], round(x[1] / 31, 1))).filter(
    lambda x: x[1] > 10).sortBy(lambda t: t[0]).sortBy(lambda t: t[1], ascending=False).map(lambda x: x[0])
for row in data_rdd.take(110):
    print(row)
