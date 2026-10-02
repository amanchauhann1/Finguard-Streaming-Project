from pyspark import pipelines as dp
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

@dp.table(
    name= 'finguard.silver.fraud_watchlist',
    comment= 'cleaned fraud watchlist'
)
def fraud_watchlist_silver() -> DataFrame:
    bronze_df = (spark.readStream
                    .table('finguard.bronze.fraud_watchlist')
                    
                    )
    
    cleaned_df = bronze_df.select(
        f.upper(col('watchlist_id')).alias('watchlist_id'),
        col('watch_type'),
        f.upper(col('entity_id')).alias('entity_id'),
        f.upper(col('risk_level')).alias('risk_level'),
        f.upper(col('action')).alias('action'),
        col('reason_code'),
        col('reason_description'),
        col('status'),
        f.to_timestamp(col('effective_from'), 'dd-MMM-yyyy HH:mm:ss').alias('effective_from'),
        col('reported_by'),
        col('reported_source'),
        col('country'),
        col('city'),
        col('source_file'),
        col('ingestion_timestamp').alias('bronze_timestamp'),
        f.current_timestamp().alias('silver_ingestion_timestamp')
    )
    
    return cleaned_df