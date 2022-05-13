"""
    * File author: Ibrahima DIOP
    Compagny : Doubango AI
    linkedin : https://www.linkedin.com/in/ibrahima-diop-82636462/
    Email : ibrahimadiop.idp@gmail.com
    * License: For non commercial use only.
    * Source code: https://github.com/Iaddiop/ultimateALPR_data_extraction/blob/master/ETL.py

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
    # Create bodyStyle fields and DataFrame, allways get the first items in array, and write the output to parquet file
    bodyStyleFields = [F.monotonically_increasing_id(), "frame_id", "bodyStyle.name", "bodyStyle.confidence", "timestamp"]
    bodyStyleFieldsName = ["bodyStyle_id","frame_id", "Name", "Confidence", "createDateTime"]

    bodyStyleDF = explodeDFL1.select(F.col("frame_id"),\
                                    F.col("plate.car.bodyStyle")[0].alias("bodyStyle"),\
                                    F.col("timestamp"))

    bodyStyle = bodyStyleDF.select(*bodyStyleFields).toDF(*bodyStyleFieldsName)
    bodyStyle.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'bodyStyle')

    # creta color fields and DataFrame, allways get the first items in array, and write the output to parquet file
    colorFields = [F.monotonically_increasing_id(), "frame_id", "color.name", "color.confidence", "timestamp"]
    colorFieldsName = ["color_id","frame_id", "Name", "Confidence", "createDateTime"]
    colorDF = explodeDFL1.select(F.col("frame_id"),\
                                F.col("plate.car.color")[0].alias("color"),\
                                F.col("timestamp"))

    color = colorDF.select(*colorFields).toDF(*colorFieldsName)
    color.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'color')

    # Create makeModelYear fields and DataFrame, allways get the first items in array, and write the output to parquet file
    makeModelYearFields = [F.monotonically_increasing_id(), "frame_id", "makeModelYear.confidence", 
    "makeModelYear.make", "makeModelYear.year", "makeModelYear.model", "timestamp"]
    cmakeModelYearFieldsName = ["makeModelYear_id","frame_id", "model", "make","year", "Confidence", "createDateTime"]

    makeModelYearDF = explodeDFL1.select(F.col("frame_id"),\
                                F.col("plate.car.makeModelYear")[0].alias("makeModelYear"),\
                                F.col("timestamp"))

    makeModelYear = makeModelYearDF.select(*makeModelYearFields).toDF(*cmakeModelYearFieldsName)
    makeModelYear.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'makeModelYear')

    # Create car fields and DataFrame, we save here the 1st, 4th and the 5th items, and write the output to parquet file
    carFields = [F.monotonically_increasing_id(), "frame_id", 
                F.col("plate.car.color.name")[0], F.col("plate.car.bodyStyle.name")[0],F.col("plate.car.makeModelYear.make")[0], "plate.text",
                F.col("plate.car.warpedBox")[0], F.col("plate.car.warpedBox")[1], F.col("plate.car.warpedBox")[4], F.col("plate.car.warpedBox")[5], "timestamp"]

    carFieldsName = ["car_id", "frame_id", "color", "bodyStyle", "plateText","make", "warpedBoxV1", "warpedBoxV2", "warpedBoxV3","warpedBoxV4", "createDateTime"]

    car = explodeDFL1.select(*carFields).toDF(*carFieldsName).dropDuplicates(["plateText", "color", "bodyStyle",  "make", "createDateTime"])

    # II - plate caracterisques
    # Create country DataFrame, and write the output to parquet file
    countryFields = [F.monotonically_increasing_id(), "frame_id", "plateCountry.name", "plateCountry.state", 
    "plateCountry.code", "plateCountry.confidence","timestamp"]
    countryFieldsName = ["country_id","frame_id", "country", "countryState", "countryCode","countryConfidence", "createDateTime"]

    countryDF = explodeDFL1.select(F.col("frame_id"),\
                                F.col("plate.country")[0].alias("plateCountry"),\
                                F.col("timestamp"))

    country = countryDF.select(*countryFields).toDF(*countryFieldsName)
    country.write.partitionBy("createDateTime").mode("overwrite").parquet(outputData + 'country')

    # Create late DataFrame, and write the output to parquet file, we save here the 1st item in the name, 1th and the 2nd items for confidences, drop duplicates and write output to parquet file
    plateFields = [F.monotonically_increasing_id(), "frame_id", F.col("plate.country.name")[0], "plate.text", 
                        F.col("plate.warpedBox")[0],F.col("plate.warpedBox")[1],F.col("plate.warpedBox")[4], 
                        F.col("plate.warpedBox")[5], F.col("plate.confidences")[0], F.col("plate.confidences")[1], "timestamp"]

    plateFieldsName = ["plateWarpedBox_id","frame_id", "countryName", "plateText", "warpedBoxV0", "warpedBoxV1", "warpedBoxV4",
                            "warpedBoxV5", "globalConfidencev1", "globalConfidencev2", "createDateTime"]

    plate = explodeDFL1.select(*plateFields).toDF(*plateFieldsName).dropDuplicates(["plateText", "countryName", "createDateTime"])


def main():
    spark = createSparkSession()
    inputData = "s3a://ultimatealpr-staging/area/"
    outputData = "s3a://lakestorage/"

    processData(spark, inputData, outputData)

    spark.stop()

if __name__ == "__main__":
    main()
