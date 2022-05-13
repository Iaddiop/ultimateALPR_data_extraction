"""
    * File author: Ibrahima DIOP
    Compagny : Doubango AI
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


def getData(spark, path):
    bodyStyle = spark.read.parquet(path + "bodyStyle/")
    car = spark.read.parquet(path + "car/")
    carWarpedBox = spark.read.parquet(path + "carWarpedBox/")
    color = spark.read.parquet(path + "color/")
    country = spark.read.parquet(path + "country/")
    makeModelYear = spark.read.parquet(path + "makeModelYear/")
    plate = spark.read.parquet(path + "plate/")
    plateWarpedBox = spark.read.parquet(path + "plateWarpedBox/")

def dataModelCheck(dataframe):
    return dataframe.printSchema()

def rowsCheck(dataframe):
    return dataframe.count() > 0

def integrityCheckCar(car, bodyStyle, color, makeModelYear):
        """
        Check the integrity of the model.
        :return: true or false if integrity is correct.
        """
        joinKey1 = [car.bodyStyle == bodyStyle.name, car.frame_id == bodyStyle.frame_id, car.createDateTime == bodyStyle.createDateTime]
        integrity_bodyStyle = car.join(bodyStyle, joinKey1, "inner") \
                             .count() == 0

        joinKey2 = [car.color == color.name, car.frame_id == color.frame_id, car.createDateTime == color.createDateTime]
        integrity_color = car.join(color, joinKey2, "inner") \
                             .count() == 0

        joinKey3 = [car.make == makeModelYear.make, car.frame_id == makeModelYear.frame_id, car.createDateTime == makeModelYear.createDateTime]
        integrity_makeModelYear = car.join(makeModelYear, joinKey2, "inner") \
                             .count() == 0
        return integrity_bodyStyle & integrity_color & integrity_makeModelYear

def integrityCheckPlate(plate, country):
        """
        Check the integrity of the model.
        :return: true or false if integrity is correct.
        """
        joinKey1 = [plate.frame_id == country.frame_id, plate.countryName == country.frame_id, plate.createDateTime == country.createDateTime]
        integrity_bodyStyle = car.join(bodyStyle, joinKey1, "inner") \
                             .count() == 0

        joinKey2 = [car.color == color.name, car.frame_id == color.frame_id, car.createDateTime == color.createDateTime]
        integrity_color = car.join(color, joinKey2, "inner") \
                             .count() == 0

        joinKey3 = [car.make == makeModelYear.make, car.frame_id == makeModelYear.frame_id, car.createDateTime == makeModelYear.createDateTime]
        integrity_makeModelYear = car.join(makeModelYear, joinKey2, "inner") \
                             .count() == 0
        return integrity_bodyStyle & integrity_color & integrity_makeModelYear

def main():
    spark = createSparkSession()
    path = "s3a://lakestorage/"

    dataframe = ["bodyStyle", "car", "carWarpedBox", "color", "country" ,"makeModelYear", "plate", "plateWarpedBox"]

    getData(spark, path)
    dataModelCheck(*dataframe)
    rowsCheck(*dataframe)
    integrityCheckCar()
    integrityCheckPlate()


    spark.stop()

if __name__ == "__main__":
    main()