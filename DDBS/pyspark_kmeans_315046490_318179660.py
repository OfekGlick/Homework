import findspark
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import Row
import numpy as np

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def wrapper(cents):
    def cal_dist(point: list):
        dist = []
        for cent in cents:
            dist.append(sum([(xi - yi) ** 2 for xi, yi in zip(point, cent)]) ** 0.5)
        import numpy as np
        temp = np.array(dist)
        dist = min(temp)
        classification = np.argmin(temp)
        return dist, classification

    return cal_dist


def kmeans_fit(data: pyspark.sql.dataframe.DataFrame, k, max_iter, q, init):
    # while max_iter >= 0:
    #     max_iter -= max_iter
    calc = wrapper(init)
    data = data.rdd.map(lambda row: (row[0], row[1], row[2], *calc(row))).sortBy(lambda x: x[3]).sortBy(
        lambda x: x[4])
    new_rdd = []
    new_cents = []
    for i in range(k):
        new_rdd.append(data.filter(lambda x: x[4] == i).map(lambda row: np.array((row[0], row[1], row[2]))))
        new_cents.append(new_rdd[i].mean())
    print(new_cents)


if __name__ == '__main__':
    spark, sc = init_spark('hw2')
    data = spark.read.parquet(r"random_data.parquet")
    k = 10
    max_iter = 100
    q = 10
    init = data.take(k)
    res = kmeans_fit(data, k, max_iter, q, init)
