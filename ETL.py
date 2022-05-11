"""
    * File author: Ibrahima DIOP
    linkedin : https://www.linkedin.com/in/ibrahima-diop-82636462/
    Email : ibrahimadiop.idp@gmail.com
    * License: For non commercial use only.
    * Source code:

"""

import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def createSparkSession():
    """
    Desccription : initiate sparkSession (configure and create or get application if it exists)
    To deal with dataframe
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark

def processData(spark, inputData, outputData):
    """
    Description : this function will be use to process data from s3 bucket log files
    Arguments :
        - spark : spark session function
        - input_data : url to access to the public s3 bucket for log files
        - output_data : url to acces to the public S3 bucket for output parquet files
    """
    # get filepath to json log files
    logFiles = inputData

    # read song data file
    df = spark.read.parquet(logFiles)

    # explode DF at level 1
    explodeDFL1 = df.select(F.col("frame_id"),\
                        F.explode("plates").alias("plate"),\
                       F.col("timestamp").cast("timestamp"))
    
    # I - Car caracterisques
    # Create bodyStyle fields and DataFrame, all ways get the first items in array, and write the output to parquet file
    bodyStyleFields = ["frame_id", "bodyStyle.name", "bodyStyle.confidence", "timestamp"]
    bodyStyleFieldsName = ["frameID", "bodyStyleName", "bodyStyleConfidence", "createDateTime"]

    bodyStyleDF = explodeDFL1.select(F.col("frame_id"),\
                                    F.col("plate.car.bodyStyle")[0].alias("bodyStyle"),\
                                    F.col("timestamp"))

    bodyStyle = bodyStyleDF.select(*bodyStyleFields).toDF(*bodyStyleFieldsName)
    bodyStyle.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'bodyStyle')

    # creta color fields and DataFrame, all ways get the first items in array, and write the output to parquet file
    colorFields = ["frame_id", "color.name", "color.confidence", "timestamp"]
    colorFieldsName = ["frameID", "colorName", "colorConfidence", "createDateTime"]
    colorDF = explodeDFL1.select(F.col("frame_id"),\
                                F.col("plate.car.color")[0].alias("color"),\
                                F.col("timestamp"))

    color = colorDF.select(*colorFields).toDF(*colorFieldsName)
    color.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'color')

    # Create makeModelYear fields and DataFrame, all ways get the first items in array, and write the output to parquet file
    makeModelYearFields = ["frame_id", "makeModelYear.model", "makeModelYear.confidence", "makeModelYear.make", "makeModelYear.year", "timestamp"]
    cmakeModelYearFieldsName = ["frameID", "model", "makeModelConfidence", "make","year", "createDateTime"]

    makeModelYearDF = explodeDFL1.select(F.col("frame_id"),\
                                F.col("plate.car.makeModelYear")[0].alias("makeModelYear"),\
                                F.col("timestamp"))

    makeModelYear = makeModelYearDF.select(*makeModelYearFields).toDF(*cmakeModelYearFieldsName)
    makeModelYear.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'makeModelYear')

    # Create arpedBoxCar fields and DataFrame, we save here the 1st, 4th and the 5th items, and write the output to parquet file
    carWarpedBoxFields = ["frame_id", F.col("plate.car.warpedBox")[0], F.col("plate.car.warpedBox")[1], F.col("plate.car.warpedBox")[4], F.col("plate.car.warpedBox")[5], "timestamp"]
    carWarpedBoxFieldsName = ["frameID", "warpedBoxV1", "warpedBoxV2", "warpedBoxV3","warpedBoxV4", "createDateTime"]

    carWarpedBox = explodeDFL1.select(*carWarpedBoxFields).toDF(*carWarpedBoxFieldsName)
    carWarpedBox.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'carWarpedBox')

    # Create car DataFrame, and write the output to parquet file
    car = explodeDFL1.select(F.col("frame_id"),\
                          F.col("plate.car.confidence").alias("globalConfidence"),\
                          F.col("timestamp").alias("createDateTime"))
    car.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'car')

    # II - plate caracterisques
    # Create country DataFrame, and write the output to parquet file
    countryFields = ["frame_id", "plateCountry.name", "plateCountry.state", "plateCountry.code", "plateCountry.confidence","timestamp"]
    countryFieldsName = ["frameID", "country", "countryState", "countryCode","countryConfidence", "createDateTime"]

    countryDF = explodeDFL1.select(F.col("frame_id"),\
                                F.col("plate.country")[0].alias("plateCountry"),\
                                F.col("timestamp"))

    country = countryDF.select(*countryFields).toDF(*countryFieldsName)
    country.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'country')

    # Create warpedBoxPlate DataFrame, and write the output to parquet file
    platewarpedBoxFields = ["frame_id", "plate.text", F.col("plate.warpedBox")[0],F.col("plate.warpedBox")[1],F.col("plate.warpedBox")[4], F.col("plate.warpedBox")[5], "timestamp"]
    platewarpedBoxFieldsName = ["frameID", "pateText", "warpedBoxV0", "warpedBoxV1", "warpedBoxV4","warpedBoxV5", "createDateTime"]

    plateWarpedBox = explodeDFL1.select(*platewarpedBoxFields).toDF(*platewarpedBoxFieldsName)
    plateWarpedBox.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'plateWarpedBox')

    # Create late DataFrame, and write the output to parquet file
    plateFields = ["frame_id", "plate.text", F.col("plate.country.name")[0], F.col("plate.confidences")[0], F.col("plate.confidences")[1],"timestamp"]
    plateFieldsName = ["frame_id", "plateText", "countryName", "globalConfidencev1", "globalConfidencev2","createDateTime"]

    plate = explodeDFL1.select(*plateFields).toDF(*plateFieldsName)
    plate.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'plate')


def main():
    spark = createSparkSession()
    inputData = "s3a://ultimatealpr-staging/area/"
    outputData = "s3a://lakestorage/"

    processData(spark, inputData, outputData)

    spark.stop()

if __name__ == "__main__":
    main()
