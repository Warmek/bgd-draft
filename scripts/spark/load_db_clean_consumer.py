import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT", 5432)
database = "bgd"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_HOST")
KAFKA_TOPIC = "tlc"

spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

processed_stream = kafka_df.withColumn(
    "value_str",
    col("value").cast("string")
)

def process_batch(batch_df, batch_id):
    print("="*50)
    print(f"--- START PROCESSING BATCH ID: {batch_id} ---")
    batch_df.write.format("jdbc").option("url", f"jdbc:postgresql://{user}:{password}@{host}:{port}/{database}").option("dbtable", "zones").option("user", "username").option("password", "password").option("driver", "org.postgresql.Driver")
    print("--- END PROCESSING BATCH ---")
    print("="*50)


# 4. Start the Stream Query
# This starts the consumer loop. It processes data in micro-batches.
query = processed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='5 seconds') \
    .start()

# Wait for the termination of the query (keep the script running)
query.awaitTermination()
