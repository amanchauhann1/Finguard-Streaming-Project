from pyspark import pipelines as dp
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name='finguard.bronze.fraud_watchlist',
    comment='fraud_watchlist raw stream data ingested by autoloader'
)
def fraud_watchlist_bronze() -> DataFrame:
    streaming_df = (spark.readStream.format('cloudFiles')
                    .option('cloudFiles.format','json')
                    .option('cloudFiles.inferColumnTypes','True')
                    .load('/Volumes/finguard/source/fraud_watchlist/source_data/')
                    )
    
    parshed_df = streaming_df.select(
        col('watchlist_id'),
        col('watch_type'),
        col('entity_id'),
        col('risk_level'),
        col('action'),
        col('reason_code'),
        col('reason_description'),
        col('status'),
        col('effective_from'),
        col('reported_by'),
        col('reported_source'),
        col('country'),
        col('city'),
        col('_rescued_data'),
        col('_metadata.file_path').alias('source_file'),
        f.current_timestamp().alias('ingestion_timestamp')
    )

    return parshed_df