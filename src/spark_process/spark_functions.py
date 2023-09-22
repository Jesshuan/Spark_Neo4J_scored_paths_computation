
from pyspark.sql.functions import col, flatten, collect_list, explode, broadcast

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, ArrayType

from pyspark.sql import SparkSession

from variables.memory_path import MEMORY_PATHS_FOLDER

from variables.hyperparameters import NB_DRAWS, MAX_ROW_SIZE_PER_TASK

import os

#RELATIVE_LIMIT_ROWS_DF_MEMORY = 10000000


""" def spark_count(df_spark):

    df = df_spark \
        .withColumn("collection", explode(col("collection"))) \
        .groupBy("collection").count() \
        .withColumnRenamed("collection", "insee") \
        .withColumnRenamed("count", "value") \
        .toPandas()
    
    return df """


def spark_comparison_to_df_memory_paths(df_batch):

    spark = SparkSession.builder.master("local").getOrCreate()

    df_shuffle_schema = StructType([ StructField("source", IntegerType(), True)\
                       ,StructField("target", IntegerType(), True)])

    df_spark_batch = spark.createDataFrame(df_batch, schema=df_shuffle_schema)

    df_spark_memory = spark.read.parquet(MEMORY_PATHS_FOLDER)\
                            .select("source", "target")\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory")

    #df_join = df_memory.join(broadcast(df_shuffle) , ["source", "target"], "right").toPandas()

    condition = (df_spark_batch.source == df_spark_memory.source_memory) & (df_spark_batch.target == df_spark_memory.target_memory)

    df_join = df_spark_batch.join(df_spark_memory, condition, how='left').toPandas()

    print(df_join.head())

    print(len(df_join))

    return df_join.loc[df_join["source_memory"].isna(),["source", "target"]], df_join.loc[~df_join["source_memory"].isna(),["source", "target"]]




def spark_append_to_memory_paths(result_paths_list):

    spark = SparkSession.builder.master("local").getOrCreate()

    sc = spark.sparkContext

    RDDmap = sc.parallelize(result_paths_list, numSlices = (NB_DRAWS // MAX_ROW_SIZE_PER_TASK) + 1)

    df_result_spark = RDDmap.map(lambda m : (m["source"], \
                                        m["target"], \
                                        m["path"], \
                                        m["totalCost"], \
                                        m["costs"])).toDF(["source","target","path","totalCost","costs"])
    
    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            print("Append results to memory path...")

            df_spark_memory = spark.read.parquet(MEMORY_PATHS_FOLDER)\
            
            df_concatenate = df_spark_memory.unionByName(df_result_spark)
            
            df_concatenate.repartition("source").write.mode("overwrite").parquet(MEMORY_PATHS_FOLDER)

        else:

            print("Overwrite/write results to memory path...")
            
            df_result_spark.repartition("source").write.mode("overwrite").parquet(MEMORY_PATHS_FOLDER)

    else:
        print("Overwrite/write results to memory path...")

        df_result_spark.repartition("source").write.mode("overwrite").parquet(MEMORY_PATHS_FOLDER)

     
        #df_result.to_parquet(DF_MEMORY_PATH, engine="fastparquet", append=True)

        #df_result.repartition(nb_part).write.mode("append").parquet(DF_MEMORY_PATH)

        '''df_memory_new = df_memory.unionByName(df_result)

        df_memory_new.write.mode("overwrite").parquet(DF_MEMORY_PATH)'''


        """ df.to_parquet(file_path, engine='fastparquet')
            else:
                df.to_parquet(file_path, engine='fastparquet', append=True) """
    



