from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructField, StructType, DateType
from pyspark.sql.functions import current_timestamp, col, concat, lit

spark = SparkSession.builder.appName("Ingest Drivers File")\
                            .config("spark.jars", "C:\\Users\\bryan\\Downloads\\postgresql-42.7.4.jar")\
                            .getOrCreate()

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

drivers_df = spark.read.schema(drivers_schema).json('raw\drivers.json')

drivers_renamed = drivers_df.withColumnRenamed('driverId', 'driver_id')\
                            .withColumnRenamed('driverRef', 'driver_ref')\
                            .withColumnRenamed('name', 'driver_name')\
                            .withColumnRenamed('nationality', 'driver_nationality')\
                            .withColumn('driver_name', concat(col('driver_name.forename'), lit(' '), col('driver_name.surname')))

driver_df_final = drivers_renamed.drop(col('url'))

db_url = "jdbc:postgresql://localhost:5432/f1_processed"  
db_properties = {
    "user": "postgres",       
    "password": "!Walalng28",   
    "driver": "org.postgresql.Driver"
}
# To access postgres
# psql -U postgres

driver_df_final.write.jdbc(url=db_url, table = 'drivers', properties = db_properties, mode = 'overwrite')

driver_df_final.show()
spark.stop()
