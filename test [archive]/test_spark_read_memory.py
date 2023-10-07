import time

from spark_process.spark_and_memory_paths import spark_get_all_memory_paths

from delta import *


df_spark = spark_get_all_memory_paths()

print(f"Count : {df_spark.count()}")


