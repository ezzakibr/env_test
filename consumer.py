from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


spark = SparkSession.builder \
    .appName("KafkaCassandraPipeline") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("timestamp", DoubleType()),
    StructField("value", DoubleType()),
    StructField("category", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_topic") \
    .load()


parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


processed_df = parsed_df \
    .withColumn("value", col("value") * 2)  


query = processed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("keyspace", "test_keyspace") \
    .option("table", "messages") \
    .start()


query.awaitTermination()