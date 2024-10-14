from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructField, StructType, DateType
from pyspark.sql.functions import current_timestamp, col, concat, lit, to_timestamp

spark = SparkSession.builder.appName('Ingest races file')\
                            .config('spark.jars', "C:\\Users\\bryan\\Downloads\\postgresql-42.7.4.jar")\
                            .getOrCreate()

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) ])

races_df = spark.read \
                .option("header", True) \
                .schema(races_schema) \
                .csv('raw\\races.csv')

races_with_timestamp = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

races_with_ingestion_date_df = races_with_timestamp.withColumn('ingestion_date', current_timestamp())

races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

db_url = "jdbc:postgresql://localhost:5432/f1_processed"  
db_properties = {
    "user": "postgres",       
    "password": "!Walalng28",   
    "driver": "org.postgresql.Driver"
}

races_selected_df.write.jdbc(url=db_url, properties = db_properties, table = 'races').mode('overwrite').partitionBy('race_year')


races_selected_df.show()
spark.stop()