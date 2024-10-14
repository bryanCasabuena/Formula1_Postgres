import os 
from pyspark.sql import SparkSession

def spark_session(app_name):
    return SparkSession.builder.appName(app_name)\
                               .config("spark.jars", "C:\\Users\\bryan\\Downloads\\postgresql-42.7.4.jar")\
                               .getOrCreate()

def connect_db():
    
