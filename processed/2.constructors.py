from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructField, StructType
from pyspark.sql.functions import current_timestamp, col

spark = SparkSession.builder.appName("Ingest constructors File")\
                            .config("spark.jars", "C:\\Users\\bryan\\Downloads\\postgresql-42.7.4.jar").getOrCreate()

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
.schema(constructors_schema) \
.json('raw\constructors.json')

constructor_dropped_df = constructor_df.drop(col('url'))

constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") 

db_url = "jdbc:postgresql://localhost:5432/f1_processed"  
db_properties = {
    "user": "postgres",       
    "password": "!Walalng28",   
    "driver": "org.postgresql.Driver"
}

constructor_renamed_df.write.jdbc(url=db_url, table = 'constructors', properties = db_properties, mode = 'overwrite')


constructor_renamed_df.show()
spark.stop()