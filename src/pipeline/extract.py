import json
import yaml


"""
extract.py
~~~~~~~~~~

Python module providing utilities to extract data 
from csv and json and generate Spark Dataframe

"""


def csv_to_df(spark, filepath):
    """
    Import csv file as pyspark Dataframe
    :param spark: sparkSession
    :param filepath: path of the input file
    :return: generated Dataframe
    """
    df = spark.read.option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv(filepath)
    return df


def json_to_df(spark, filepath):
    """
    Import json file as pyspark Dataframe
    :param spark: Spark session
    :param filepath: path of the input file
    :return: generated Dataframe
    """
    json_data = yaml.load(open(filepath), Loader=yaml.FullLoader)

    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(json_data)]))
    return df
