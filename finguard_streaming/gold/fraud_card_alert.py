from pyspark import pipelines as dp
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

@dp.table(
    name= 'finguard.gold.fraud_card_alert',
    comment= 'alert when card is detected fraud'
)
def fraud_card_alert() -> DataFrame:
    transactions = spark.readStream.table('finguard.silver.transactions')
    fraud_watchlist = spark.readStream.table('finguard.silver.fraud_watchlist')

    customers = spark.read.table('finguard.silver.customers')

    transactions_with_watermark = transactions.withWatermark('transaction_timestamp','5 minutes')
    fraud_watchlist_with_watermark = fraud_watchlist.withWatermark('effective_from','5 minutes')

    fraud_detected = (
        transactions_with_watermark.join(fraud_watchlist_with_watermark,transactions_with_watermark.card_number == fraud_watchlist_with_watermark.entity_id,'inner')
    ).join(customers,transactions_with_watermark.customer_id == customers.customer_id,'left').select(

        # alert identification
        transactions_with_watermark.transaction_id,
        transactions_with_watermark.customer_id,
        customers.email.alias('customer_email'),
        f.concat_ws(' ',customers.first_name,customers.last_name).alias('customer_name'),
        transactions_with_watermark.card_number,
        transactions_with_watermark.amount,
        transactions_with_watermark.currency,
        transactions_with_watermark.merchant_name,
        transactions_with_watermark.merchant_category,
        transactions_with_watermark.transaction_type,
        transactions_with_watermark.payment_channel,
        transactions_with_watermark.device_id,
        transactions_with_watermark.city.alias("transaction_city"),
        transactions_with_watermark.country.alias("transaction_country"),
        transactions_with_watermark.transaction_timestamp,
        transactions_with_watermark.is_international,
        transactions_with_watermark.status.alias("transaction_status"),

        # fraud watchlist details
            f.col("watchlist_id"),
            f.col("watch_type"),
            f.col("risk_level"),
            f.col("action"),
            f.col("reason_code"),
            f.col("reason_description"),
            f.col("effective_from").alias("watchlist_effective_from"),
            f.col("reported_by"),
            f.col("reported_source"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")

    )

    return fraud_detected

















