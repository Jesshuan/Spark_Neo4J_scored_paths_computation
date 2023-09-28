import time

from spark_process.spark_and_memory_paths import spark_get_all_memory_paths


while True:

    print('-------------------------')
    
    df_spark = spark_get_all_memory_paths()

    print(f"Count : {df_spark.count()}")

    print(df_spark.show(5))

    time.sleep(5)

