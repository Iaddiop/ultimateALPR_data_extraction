"""
File author: Ibrahima DIOP
Company: Doubango AI
LinkedIn: https://www.linkedin.com/in/ibrahima-diop-82636462/
Email: ibrahimadiop.idp@gmail.com
License: For non-commercial use only.
Source code: https://github.com/Iaddiop/ultimateALPR_data_extraction
"""

import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration and set environment variables
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create and configure a Spark session.
    """
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise


def extract_body_style(df):
    """
    Extract and process body style data.
    """
    body_style_df = df.select(
        F.col("frame_id"),
        F.col("plate.car.bodyStyle")[0].alias("bodyStyle"),
        F.col("timestamp")
    )
    body_style_fields = [F.monotonically_increasing_id(), "frame_id", "bodyStyle.name", "bodyStyle.confidence",
                         "timestamp"]
    body_style_fields_name = ["bodyStyle_id", "frame_id", "Name", "Confidence", "createDateTime"]
    return body_style_df.select(*body_style_fields).toDF(*body_style_fields_name)


def extract_color(df):
    """
    Extract and process color data.
    """
    color_df = df.select(
        F.col("frame_id"),
        F.col("plate.car.color")[0].alias("color"),
        F.col("timestamp")
    )
    color_fields = [F.monotonically_increasing_id(), "frame_id", "color.name", "color.confidence", "timestamp"]
    color_fields_name = ["color_id", "frame_id", "Name", "Confidence", "createDateTime"]
    return color_df.select(*color_fields).toDF(*color_fields_name)


def extract_make_model_year(df):
    """
    Extract and process make_model_year data.
    """
    make_model_year_df = df.select(
        F.col("frame_id"),
        F.col("plate.car.makeModelYear")[0].alias("makeModelYear"),
        F.col("timestamp")
    )
    make_model_year_fields = [F.monotonically_increasing_id(), "frame_id", "makeModelYear.confidence",
                              "makeModelYear.make", "makeModelYear.year", "makeModelYear.model", "timestamp"]
    make_model_year_fields_name = ["makeModelYear_id", "frame_id", "model", "make", "year", "Confidence",
                                   "createDateTime"]
    return make_model_year_df.select(*make_model_year_fields).toDF(*make_model_year_fields_name)


def extract_car(df):
    """
    Extract and process car data .
    """
    car_fields = [F.monotonically_increasing_id(), "frame_id",
                  F.col("plate.car.color.name")[0], F.col("plate.car.bodyStyle.name")[0],
                  F.col("plate.car.makeModelYear.make")[0], "plate.text",
                  F.col("plate.car.warpedBox")[0], F.col("plate.car.warpedBox")[1], F.col("plate.car.warpedBox")[4],
                  F.col("plate.car.warpedBox")[5], "timestamp"]

    car_fields_name = ["car_id", "frame_id", "color", "bodyStyle", "plateText", "make", "warpedBoxV1", "warpedBoxV2",
                       "warpedBoxV3", "warpedBoxV4", "createDateTime"]

    return df.select(*car_fields).toDF(*car_fields_name).dropDuplicates(
        ["plateText", "color", "bodyStyle", "make", "createDateTime"])


def extract_country(df):
    """
    Extract and process country data .
    """
    country_df = df.select(
        F.col("frame_id"),
        F.col("plate.country")[0].alias("plateCountry"),
        F.col("timestamp")
    )
    country_fields = [F.monotonically_increasing_id(), "frame_id", "plateCountry.name", "plateCountry.state",
                      "plateCountry.code", "plateCountry.confidence", "timestamp"]
    country_fields_name = ["country_id", "frame_id", "country", "countryState", "countryCode", "countryConfidence",
                           "createDateTime"]
    return country_df.select(*country_fields).toDF(*country_fields_name)


def extract_plate(df):
    """
    Extract and process plate data.
    """
    plate_fields = [F.monotonically_increasing_id(), "frame_id", F.col("plate.country.name")[0], "plate.text",
                    F.col("plate.warpedBox")[0], F.col("plate.warpedBox")[1], F.col("plate.warpedBox")[4],
                    F.col("plate.warpedBox")[5], F.col("plate.confidences")[0], F.col("plate.confidences")[1],
                    "timestamp"]

    plate_fields_name = ["plateWarpedBox_id", "frame_id", "countryName", "plateText", "warpedBoxV0", "warpedBoxV1",
                         "warpedBoxV4", "warpedBoxV5", "globalConfidencev1", "globalConfidencev2", "createDateTime"]

    return df.select(*plate_fields).toDF(*plate_fields_name).dropDuplicates(
        ["plateText", "countryName", "createDateTime"])


def process_data(spark, input_data, output_data):
    """
    Process data from input S3 bucket and write processed data to output S3 bucket.

    Arguments:
    - spark : Spark session object
    - input_data : Input data path (S3)
    - output_data : Output data path (S3)
    """

    df = spark.read.parquet(input_data)
    explode_l1_df = df.select(
        F.col("frame_id"),
        F.explode("plates").alias("plate"),
        F.col("timestamp").cast("timestamp")
    )

    # Extract and write data to respective paths
    extract_body_style(explode_l1_df).write.partitionBy("createDateTime").mode("overwrite").parquet(
        output_data + 'bodyStyle')
    extract_color(explode_l1_df).write.partitionBy("createDateTime").mode("overwrite").parquet(
        output_data + 'color')
    extract_make_model_year(explode_l1_df).write.partitionBy("createDateTime").mode("overwrite").parquet(
        output_data + 'makeModelYear')
    extract_car(explode_l1_df).write.partitionBy("createDateTime").mode("overwrite").parquet(output_data + 'car')
    extract_country(explode_l1_df).write.partitionBy("createDateTime").mode("overwrite").parquet(
        output_data + 'country')
    extract_plate(explode_l1_df).write.partitionBy("createDateTime").mode("overwrite").parquet(
        output_data + 'plate')


def main():
    """
    Main function to initiate the data processing.
    """
    spark = create_spark_session()
    input_data = "s3a://ultimatealpr-staging/area/"
    output_data = "s3a://lakestorage/"

    process_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
