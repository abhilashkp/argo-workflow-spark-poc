from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

data = range(1, 1000)
rdd = spark.sparkContext.parallelize(data)
squared = rdd.map(lambda x: x * x)

print(squared.collect())
