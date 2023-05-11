from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window,from_json
import configparser
#from argparse import ArgumentParser, FileType
#from pyspark.sql.types import StructType, StructField,StringType,IntegerType
from pyspark.sql import functions as F

if __name__ == '__main__':
    
    scala_version = '2.12'
    spark_version = '3.1.2'

    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.2.1',
        'org.postgresql:postgresql:42.6.0'
    ]

    config = configparser.ConfigParser()

    config.read('config.ini')


    topic = "topic_1"
    GROUP_ID = "group_10"
    apikey = config.get('defaulf', 'sasl.username')
    apisecret = config.get('defaulf', 'sasl.password')
    bootstrap_server = config.get('defaulf', 'bootstrap.servers')

    # configs = {
    #     'kafak.bootstrap.servers': "pkc-41p56.asia-south1.gcp.confluent.cloud:9092",
    #     "kafka.security.protocol": "SASL_SSL",
    #     "kafka.ssl.endpoint.identification.algorithm": "https",
    #     "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(apikey,apisecret),
    #     "kafka.sasl.mechanism": "PLAIN",
    #     "startingOffsets": "earliest",
    #     "failOnDataLoss": "false",
    #     "kafka.group.id": GROUP_ID,
    #     "subscribe": topic
    # }

    spark = SparkSession.builder.appName("StreamProcessingExample")\
    .master("local")\
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

    # kafka_df = spark.readStream \
    # .format("kafka") \
    # .options(**configs)\
    # .load()

    kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server)\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.ssl.endpoint.identification.algorithm", "https")\
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(apikey,apisecret))\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("startingOffsets", "earliest")\
    .option("kafka.group.id", GROUP_ID)\
    .option("subscribe", topic)\
    .option("failOnDataLoss", "false")\
    .load()

    

    value_df = kafka_df.selectExpr("CAST(value AS STRING)")
    

    schema = f"ID STRING, NAME STRING, SPEND DOUBLE, TIMESTAMP TIMESTAMP"

    parsed_data = value_df.selectExpr(f"from_json(value, '{schema}') AS data") \
    .select("data.*") \
    .select(col("ID"), col("name"), col("SPEND").cast("double"), col("TIMESTAMP").cast("timestamp").alias("event_time"))
  

    windowed_df = parsed_data \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "10 minutes"), "ID") \
    .agg(F.max("SPEND").alias("max_spend"), F.min("SPEND").alias("min_spend"), F.avg("SPEND").alias("avg_spend")) \
    .select("ID", "window.start", "window.end", "max_spend", "min_spend", "avg_spend")


    # query = windowed_df \
    #     .writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()
    
    connectionProperties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
    }

    url = "jdbc:postgresql://postgres:5432/postgres"    

    def writeToPostgres(batch_df, batch_id):

        batch_df.write.jdbc(url=url, table="spend", mode="append", properties=connectionProperties)
        
    query = windowed_df \
    .writeStream \
    .foreachBatch(writeToPostgres) \
    .outputMode("update")\
    .start()

    query.awaitTermination()


