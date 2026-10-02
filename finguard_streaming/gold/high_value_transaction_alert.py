from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name = "finguard.gold.high_value_transaction_alert",
    comment = "Alerts when transaction is performed at higher value than it is configured by customer "
)
def high_value_transaction_alert() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions")
    customers = spark.read.table("finguard.silver.customers")

    joined_df = transactions.join(customers,transactions.customer_id == customers.customer_id,"left")\
                            .filter(col("amount")>col("transaction_limit"))\
                            .select(
                                f.concat_ws("-",f.lit("ALERT"),col("transaction_id")).alias("alert_id"),
                                f.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                                f.current_timestamp().alias("alert_timestamp"),
                                transactions.transaction_id,
                                transactions.customer_id,
                                customers.email.alias("customer_email"),
                                f.concat_ws(" ",col("first_name"),col("last_name")).alias("customer_name"),
                                transactions.amount.alias("transaction_amount"),
                                customers.transaction_limit,
                                transactions.currency,
                                transactions.merchant_name,
                                transactions.merchant_category,
                                transactions.transaction_type,
                                transactions.payment_channel,
                                transactions.country,
                                transactions.city,
                                transactions.is_international,
                                transactions.transaction_timestamp,
                                transactions.status
                                )
    
    return joined_df
