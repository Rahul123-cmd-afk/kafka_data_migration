from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-cluster-kafka-bootstrap:9093") \
    .option("subscribe", "pg.public.users") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="my-user" password="vywOr5qAcKsjK5Nf9Hsa4Oxpjt1NBiTv";'
    ) \
    .option("kafka.ssl.truststore.location", "/etc/kafka/ca.p12") \
    .option("kafka.ssl.truststore.password", "bfY9evmCpBIu") \
    .option("kafka.ssl.truststore.type", "PKCS12") \
    .option("startingOffsets", "earliest") \
    .load()

data = df.selectExpr("CAST(value AS STRING)")

query = data.writeStream \
    .format("parquet") \
    .option("path", "s3a://bronze/kafka-data") \
    .option("checkpointLocation", "s3a://checkpoints/kafka-checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
