import os
from pyspark.sql import SparkSession

def main():
    print("--------------------------------------------------------------")
    print(f"PYTHON VERSION: {os.popen('python --version').read().strip()}")
    print(f"JDK VERSION: {os.popen('java -version 2>&1').read().strip()}")
    print("--------------------------------------------------------------")

    spark = SparkSession.builder \
        .appName("PySpark Uygulaması") \
        .getOrCreate()

    df = spark.read.csv('./data/test.csv', header=True, inferSchema=True)

    df.show()

    spark.stop()

if __name__ == "__main__":
    main()