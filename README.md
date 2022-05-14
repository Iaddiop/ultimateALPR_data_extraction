# ultimateALPR data lake in AWS S3 with AWS Glue Crawler AWS EMR cluster and Spark

Doubango AI is a company build artificial intelligence solutions and apps using state of the art deep learning and computer vision technologies. Providing 3D Face liveness check (anti-spoofing), ANPR/ALPR, MRZ/MRTD, ScanToPay, MICR, CBIR, ICR, OCR, SceneText for smart cities solutions.

Some user’s feedbacks would like to analyze the output logs of the ANPR/ALPR solution. To achieve this task, the support team needs to build an analysis solution to visualize events in Grafana or in web application.

Before to build the analytical solution, the output logs of ANPR/ALPR need to be process, transform and stored.

We chose the AWS technologies to process, transform and store the data.

## Steps to perform car plate recognition, extract information, transform it and store data

The log files are in local machine and will be load in S3, then we will process the ETL on a EMR cluster and finally store the results to a data lake in a S3 bucket:

<p align="center">
  <img src="./images/diagram.png">
</p>

1- License Plate Recognition: We assume this step will not be covered in this topic, but we will use it for diagram understanding:
- Collect the images: 251,526 images, each picture could have 1 or several cars, trucks or motorcycles
- Process license plate recognizer with python SDK. This will give us as many JSON log files as images (251,526) to store in local disk.

2- Load JSON log files from local to `ultimatealpr` S3 bucket.

3- Transform JSON log files from `ultimatealpr` S3 bucket to Parquet format by using AWS Glue Crawler and store them to `ultimatealpr_staging` S3 bucket.

4- Extract and transform some patterns in the files that are in `ultimatealpr_staging` S3 bucket using Spark on an EMR cluster.

5- Load results of the extraction and the transformation as datasets to a `lakestorage` S3 bucket.

6- Validate data on Jupyter Notebook with spark.


## Data modeling: why we chosen a data lake storage instead a DWH?

1- Data structure:
The JSON output files given by ANPR/ALPR are deeply nested like this:

<p align="center">
  <img src="./images/schema.PNG">
</p>

This JSON structure could change in the future. A DWH (a relational database technology) stores information in a hard schema that is not expected to changes or can be change not in ease way.
By choosing AWS S3 as the storage system, that help us to store the JSON files as are or transform them to Parquet format datasets. The parquet format, will allow us to infer the data schema on read, we can also change or update the schema in the future without any impact:

<p align="center">
  <img src="./images/explode_files_v2.png">
</p>

2- Costs:
If we have chosen a DWH technology, we probably chosen an AWS database solution (example: Redshift, DynamoDB). This strategy is more expensive than choosing an AWS S3 bucket.
S3 prices :
| S3 Standard -   General purpose storage for any type of data, typically used for frequently   accessed data | Price           |
|-------------------------------------------------------------------------------------------------------------|-----------------|
| First 50 TB / Month                                                                                         | $0.023 per   GB |
| Next 450 TB / Month                                                                                         | $0.022 per   GB |
| Over 500 TB / Month                                                                                         | $0.021 per   GB |

RDS prices :
| RDS for PostgreSQL   - General Purpose (SSD) Storage | Price           |
|------------------------------------------------------|-----------------|
| Monthly                                              | $0.115 per   GB |

## How to run this project:

1- Configure a AWS Glue job to convert JSON log files to parquet from `ultimatealpr` S3 bucket (data source) to `ultimatealpr_staging` S3 bucket (data target):

<p align="center">
  <img src="./images/Glue_crawler.PNG">
</p>

Set:
-  data source: `ultimatealpr` S3 bucket
-  data target: `ultimatealpr-staging` S3 bucket
-  chose Spark for the type of ETL
-  use 10 workers (G.1x type) for the transformation 
-  portion key : timestamp (output 141 blocks)

2- [Create EMR cluster](https://medium.com/towards-data-science/how-to-create-and-run-an-emr-cluster-using-aws-cli-3a78977dc7f0)
- chose EMR cluster instance type: m5.xlarge (more than enough)
- with 3 cores (1 driver and 2 executors)
- copy and paste the `etl.py` script and your `.pem` key to the EMR cluster at home directory
- submit the ETL Spark job in the cluster (we chose `yarn` as cluster manager)

3- Data validation 
We have 2 ways to validate de quality of the data and check results:
-  Glue crawler --> Glue catalogue --> SQL query with Athena
-  Or directely read parquet files from `ultimatealpr-staging` bucket in jupyter notebook with spark
We decide to proceed with the validation in jupyther notebook `Test.ipynb`

## Performances and cost:

1- Loading JSON files from local to `ultimatealpr` S3 bucket:
-  took hours, the performances could be improved by using AWS Data Sync
-  cost: request S3 services
    -  Put: $2.21 for 401,515 Requests ($0.0055 per 1,000 requests)
    -  Get: $2.44 for 5,549,535 Requests ($0.0044 per 10,000 requests)

2- Transform data with AWS Glue job:
- took 9 minutes 
- cost: $0.65 for 1.48 Data Process Unit (DPU : $0.44 per unit)

3- ETL with Spark on EMR cluster:
- the Spark job took 1.4 minutes
- cost: $0.048 per hour by choosing m5.xlarge instance type

## Data management:

1-	Data dictionary

| **Table**         | **Column name**    | **Column type**             | **Description**                                                                      |
|-------------------|--------------------|-----------------------------|--------------------------------------------------------------------------------------|
| **bodyStyle**     | bodyStyle_id       | long (nullable = true)      | identity key                                                                         |
|                   | frame_id           | integer (nullable = true)   | same frame could have 1 or 1+ car                                                    |
|                   | Name               | string (nullable = true)    | body style name (bus, van, suv ..)                                                   |
|                   | Confidence         | double (nullable = true)    | the percent (%) of the confidence of the Vehicule Body Style Recognition(VBSR)       |
|                   | createDateTime     | timestamp (nullable = true) | creation date (timestamp)                                                            |
| **car**           | car_id             | long (nullable = true)      | identity key                                                                         |
|                   | frame_id           | integer (nullable = true)   | same frame could have 1 or 1+ car                                                    |
|                   | color              | string (nullable = true)    | color name                                                                           |
|                   | bodyStyle          | string (nullable = true)    | body style name (bus, van, suv ..)                                                   |
|                   | plateText          | string (nullable = true)    | the licence plate text                                                               |
|                   | make               | string (nullable = true)    | the percent (%) confidence of the Vehicule Make Model Recognition (VMMR)             |
|                   | warpedBoxV1        | double (nullable = true)    | warped box value 1 for car recognition                                               |
|                   | warpedBoxV2        | double (nullable = true)    | warped box value 2 for car recognition                                               |
|                   | warpedBoxV3        | double (nullable = true)    | warped box value 5 for car recognition                                               |
|                   | warpedBoxV4        | double (nullable = true)    | warped box value 6 for car recognition                                               |
|                   | createDateTime     | timestamp (nullable = true) | creation date (timestamp)                                                            |
| **color**         | color_id           | long (nullable = true)      | identity key                                                                         |
|                   | frame_id           | integer (nullable = true)   | same frame could have 1 or 1+ car                                                    |
|                   | Name               | string (nullable = true)    | color name                                                                           |
|                   | Confidence         | double (nullable = true)    | the percent (%) of the confidence of the Vehicule Color Recognition (VCR)            |
|                   | createDateTime     | timestamp (nullable = true) | creation date (timestamp)                                                            |
| **Country**       | country_id         | long (nullable = true)      | identity key                                                                         |
|                   | frame_id           | integer (nullable = true)   | same frame can have 1 or 1+ car                                                      |
|                   | country            | string (nullable = true)    | country name of the plate                                                            |
|                   | countryState       | string (nullable = true)    | country state                                                                        |
|                   | countryCode        | string (nullable = true)    | country code                                                                         |
|                   | countryConfidence  | double (nullable = true)    | the percent (%) of the confidence of the Licence Plate Country Identification (LPCI) |
|                   | createDateTime     | timestamp (nullable = true) | creation date (timestamp)                                                            |
| **makeModelYear** | makeModelYear_id   | long (nullable = true)      | identity key                                                                         |
|                   | frame_id           | integer (nullable = true)   | same frame could have 1 or 1+ car                                                    |
|                   | model              | double (nullable = true)    | name of the model (gle, golf, clio …)                                                |
|                   | make               | string (nullable = true)    | the percent (%) confidence of the Vehicule Make Model Recognition (VMMR)             |
|                   | year               | string (nullable = true)    | car marke by bmw, renault, audi ...                                                  |
|                   | Confidence         | string (nullable = true)    | year of made                                                                         |
|                   | createDateTime     | timestamp (nullable = true) | creation date (timestamp)                                                            |
| **plate**         | plate_id           | long (nullable = true)      | primary key                                                                          |
|                   | frame_id           | integer (nullable = true)   | same frame could have 1 or 1+ car                                                    |
|                   | countryName        | string (nullable = true)    | country name of the plate                                                            |
|                   | plateText          | string (nullable = true)    | the licence plate text                                                               |
|                   | warpedBoxV0        | double (nullable = true)    | warped box value 1 for car recognition                                               |
|                   | warpedBoxV1        | double (nullable = true)    | warped box value 2 for car recognition                                               |
|                   | warpedBoxV4        | double (nullable = true)    | warped box value 5 for car recognition                                               |
|                   | warpedBoxV5        | double (nullable = true)    | warped box value 6 for car recognition                                               |
|                   | globalConfidencev1 | double (nullable = true)    | global confidence of the recognition of the plate (first value)                      |
|                   | globalConfidencev2 | double (nullable = true)    | global confidence of the recognition of the plate (second value)                     |
|                   | createDateTime     | timestamp (nullable = true) | creation date (timestamp)                                                            |

2- Data validation
After running ETL and store the data to the data lake, we will check the consistency of our data model by:
- checking if the parquet files are exist in the data lake and stored correctly by checking if there are rows in each dataset
- checking the data model by making join between datasets
Run the [Data validation]() notebook to see the results.

## Manage data accessibility and growth
-   In the `ultimatealpr` S3 bucket the data going to increase, but the analytical rules need 1 month data storage retention. If the data increase by 100x (events examples: Black Friday, charismas, sales discount period):
    -   1 month data retention will be stored in the `ultimatealpr` S3 bucket
    -   1 years archive in AWS S3 Glacier with a retrieval policy 
    -   Set AWS Glue job to automatically scale the number of workers
- If Doubango AI would like to build Dasboard solution: 
    -   Airflow could be used to run the pipeline to populate the dashboard which defined frequency (daily, weekly or monthly) 
    -   Daily data quality check could be performed end send email for the monitoring
- The S3 policy could be extend to allow new users and could be accessible by 100+ people in same time.

## Future improvements:
-   Live recognition with camera
-   Load data from local to S3 bucket in streaming mode or with AWS Data Sync to automate moving data between on premise to AWS S3.
-   Configure AWS Glue job to parse data by event from S3 data sources bucket to the S3 staging area `ultimatealpr-staging` bucket.

### References:
- [ultimateALPR](https://github.com/DoubangoTelecom/ultimateALPR-SDK)
- [Convert CSV / JSON files to Apache Parquet using AWS Glue](https://medium.com/searce/convert-csv-json-files-to-apache-parquet-using-aws-glue-a760d177b45f)
- [How to choose an appropriate file format for your data pipeline](https://medium.com/@montadhar/how-to-choose-an-appropriate-file-format-for-your-data-pipeline-69bbfa911414)

-	[Create EMR cluster](https://medium.com/towards-data-science/how-to-create-and-run-an-emr-cluster-using-aws-cli-3a78977dc7f0)
-	[Create S3 bucket]( https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)
- [Amazon S3 prices](https://aws.amazon.com/s3/pricing/?nc1=h_ls)
- [Amazon RDS Pricing](https://aws.amazon.com/rds/pricing/?nc1=h_ls)
