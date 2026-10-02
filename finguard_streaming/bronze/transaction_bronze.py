from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f

@dp.table(
    name= "finguard.bronze.transactions",
    comment= "transaction raw streame data from kafka"
)
def transaction_bronze() -> DataFrame:
    bootstrap_servers = dbutils.secrets.get(scope="kafka_scope",key="bootstrap_servers")
    api_key = dbutils.secrets.get(scope="kafka_scope",key="api_key")
    api_secret = dbutils.secrets.get(scope="kafka_scope",key="api_secret")
    topic = dbutils.secrets.get(scope="kafka_scope",key="topic")

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

    streaming_data=(spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers",bootstrap_servers)
                .option("subscribe",topic)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas_config)
                .option("startingOffsets","earliest")
                .load())


    parshed_streaming_data = streaming_data.select( 
                f.col("key").cast("string"),
                f.col("value").cast("string"),
                f.col("topic"),
                f.col("partition"),
                f.col("offset"),
                f.col("timestamp"),
                f.current_timestamp().alias("ingestion_timestamp")

            )
    
    return parshed_streaming_data
    











