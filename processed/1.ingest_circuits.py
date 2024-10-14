from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructField, StructType
from pyspark.sql.functions import current_timestamp


spark = SparkSession.builder.appName("Ingest Circuits File")\
                            .config("spark.jars", "C:\\Users\\bryan\\Downloads\\postgresql-42.7.4.jar").getOrCreate()

schema = StructType(fields=[StructField('circuitId', IntegerType()),
                            StructField('circuitRef', StringType()),
                            StructField('name', StringType()),
                            StructField('location', StringType()),
                            StructField('country', StringType()),
                            StructField('lat', DoubleType()),
                            StructField('lng', DoubleType()),
                            StructField('alt', IntegerType()),
                            StructField('url', StringType())])


df = spark.read.csv('raw\\circuits.csv', header=True, inferSchema=True, schema=schema)
df_ingestion_date = df.withColumn('ingestion_date', current_timestamp())

df_final = df_ingestion_date.withColumnRenamed('circuitId', 'circuit_id')\
                            .withColumnRenamed('circuitref', 'circuit_ref')\
                            .withColumnRenamed('name', 'circuit_name')\
                            .withColumnRenamed('location', 'circuit_location')\
                            .withColumnRenamed('country', 'circuit_country')\


df_final.show()

db_url = "jdbc:postgresql://localhost:5432/f1_processed"  
db_properties = {
    "user": "postgres",       
    "password": "!Walalng28",   
    "driver": "org.postgresql.Driver"
}

df_final.write.jdbc(url=db_url, table="circuits", mode="overwrite", properties=db_properties)

spark.stop()
