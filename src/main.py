import argparse, importlib
from pyspark.sql import SparkSession

def main():
    """
    Entry point of the pipeline
    :return: None
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--job', type=str, required=True)
    args = parser.parse_args()

    # init Spark session
    spark = SparkSession.builder.getOrCreate()

    job_module = importlib.import_module('jobs.%s' % args.job)

    job_module.execute_job(spark)


if __name__ == '__main__':
    main()