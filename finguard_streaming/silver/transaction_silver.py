from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as f

@dp.table(
    name = "finguard.silver.transactions",
    comment="parshed and cleaned transaction data"
)
@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dp.expect_or_drop("valid_merchant_name","merchant_name IS NOT NULL")
@dp.expect("valid_amount","amount > 0")
def transaction_silver() ->DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")

    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType())
    ])

    transformed_df = bronze_df.select(
        f.from_json(col("value"),schema).alias("data"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        col("data.*"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("bronze_ingestion_timestamp"),
        f.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df