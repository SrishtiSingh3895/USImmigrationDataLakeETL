import configparser
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import countDistinct, monotonically_increasing_id, count, when, isnan, col, sum
from pyspark.sql.functions import year, month, dayofmonth, weekofyear
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import os
import boto3

'''
    Test file to check spark functionality.
'''

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

if __name__ == "__main__":
    # Reading I94 immigration data
    months = ['jan']

    spark = SparkSession.builder\
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2")\
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
    .enableHiveSupport().getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",os.environ['AWS_ACCESS_KEY_ID'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key",os.environ['AWS_SECRET_ACCESS_KEY'])
    
    for month in months:
        print("Reading month ", month)
        i94 = spark.read.format('com.github.saurfang.sas.spark').load(f"../../data/18-83510-I94-Data-2016/i94_{month}16_sub.sas7bdat")
        i94.write.mode('append').partitionBy("i94mon").parquet("s3a://udacity-srishti-capstone/input/i94.parquet")