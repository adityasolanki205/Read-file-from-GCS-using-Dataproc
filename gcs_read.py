import pyspark
from pyspark.sql import SparkSession
from google.cloud import storage


appName = "DataProc testing"
master = "local"
spark = SparkSession.builder.\
        appName(appName).\
        master(master).\
        getOrCreate()     


df = spark.read.csv("gs://dataproc-testing-pyspark/titanic.csv",header=True, inferSchema=True)
print(df.show())

