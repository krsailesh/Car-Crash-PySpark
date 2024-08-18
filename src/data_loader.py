from pyspark.sql import SparkSession

def load_data(file_path):
    spark = SparkSession.builder.appName("Crash Analysis").getOrCreate()
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def load_all_data(config):
    data = {}
    for key, path in config['data_paths'].items():
        data[key] = load_data(path)
    return data

