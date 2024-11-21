from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


spark = SparkSession.builder \
    .appName("KafkaCassandraPipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0," +
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
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

categories_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="categories", keyspace ="test_keyspace") \
    .load()




def process_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        processed = batch_df \
            .join(categories_df, batch_df.category == categories_df.code, "left") \
            .select(
                batch_df.id,
                from_unixtime(batch_df.timestamp).cast("timestamp").alias("date"),
                batch_df.value,
                batch_df.category,
                categories_df.name.alias("category_name")
            )
        
        print(f"\nProcessing batch {batch_id}:")
        processed.show()
        
        processed.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "test_keyspace") \
            .option("table", "messages_with_categories") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} successfully written to Cassandra")


query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_messages_with_categories") \
    .start()

query.awaitTermination()
