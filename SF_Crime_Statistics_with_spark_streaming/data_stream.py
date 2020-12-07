import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# TODO Create a schema for incoming resources
schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("call_date", StringType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("report_date", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("offense_date", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("common_location", StringType(), True)
                    ])


def run_spark_job(spark):
    
    print("****** Setting up streaming*******")
    
    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
        .option("subscribe","sf.crime.policecalls") \
        .option("startingOffsets","earliest") \
        .option("maxOffsetPerTrigger",200) \
        #.option("maxRatePerPartition",200) \
        .option("stopGracefullyOnShutdown","true") \
        .load()
    

    # Show schema for the incoming resources for checks
    print(">>> Showing schema for the incoming resources for checks")
    df.printSchema()

    # extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df\
                        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
                        .select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
                            .select("original_crime_type_name","disposition","call_date_time") \
                            .distinct() \
                            .withWatermark('call_date_time', "2 minutes")
    
    print('*** distinct table schema ************')
    distinct_table.printSchema()
    
    # count the number of original crime type
    agg_df = distinct_table\
                    .dropna()\
                    .groupBy("original_crime_type_name")\
                    .count()
    
    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # write output stream
    query = agg_df\
                .writeStream\
                .queryName("write_stream") \
                .outputMode('Complete')\
                .format('console')\
                .option("truncate", "false")\
                .start()\
                .awaitTermination()
    

    #  get the right radio code json path
    print(">> Getting the right radio code json path ******")
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)
    
    print(">> RADIO_CODE_DF Schema")
    radio_code_df.printSchema()
    
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df \
                    .queryName("join_aggDF_radioCodeDF") \
                    .join(radio_code_df, "disposition") \
                    .writeStream \
                    .outputMode('Complete')\
                    .format("console") \
                    .option("truncate", "false")\
                    .start()
    
    
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
                    .builder \
                    .master("local[*]") \
                    .appName("KafkaSparkStructuredStreaming") \
                    .config("spark.ui.port", 3000) \
                    .getOrCreate()
    

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
