
from pyspark.sql.functions import col, flatten, collect_list, explode, broadcast

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, ArrayType

from pyspark.sql import SparkSession

from variables.memory_path import MEMORY_PATHS_FOLDER

from variables.hyperparameters import NB_DRAWS, MAX_ROW_SIZE_PER_TASK

import os

from delta import *

#RELATIVE_LIMIT_ROWS_DF_MEMORY = 10000000


""" def spark_count(df_spark):

    df = df_spark \
        .withColumn("collection", explode(col("collection"))) \
        .groupBy("collection").count() \
        .withColumnRenamed("collection", "insee") \
        .withColumnRenamed("count", "value") \
        .toPandas()
    
    return df """

def spark_get_all_memory_paths():

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    df_spark_memory = spark.read.format("delta").load(MEMORY_PATHS_FOLDER)\
                            .dropDuplicates(["source", "target"])
    
    return df_spark_memory



def spark_get_a_df_memory_paths_selection_from_batch(batch_list):

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    '''spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local").getOrCreate()'''

    df_shuffle_schema = StructType([ StructField("source", IntegerType(), True)\
                       ,StructField("target", IntegerType(), True)])
    
    df_spark_batch = spark.createDataFrame(batch_list, schema=df_shuffle_schema)

    df_spark_memory = spark.read.format("delta").load(MEMORY_PATHS_FOLDER)\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory")
    
    condition = (df_spark_batch.source == df_spark_memory.source_memory) & (df_spark_batch.target == df_spark_memory.target_memory)

    df_join = df_spark_batch.join(df_spark_memory, condition, how='left').select(["source","target","path","totalCost","costs"])

    df_spark_memory.unpersist()

    df_spark_batch.unpersist()

    return df_join


def spark_comparison_to_df_memory_paths(df_batch):

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    df_shuffle_schema = StructType([ StructField("source", IntegerType(), True)\
                       ,StructField("target", IntegerType(), True)])

    df_spark_batch = spark.createDataFrame(df_batch, schema=df_shuffle_schema)

    df_spark_memory = spark.read.format("delta").load(MEMORY_PATHS_FOLDER)\
                            .select("source", "target")\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory")

    #df_join = df_memory.join(broadcast(df_shuffle) , ["source", "target"], "right").toPandas()

    condition = (df_spark_batch.source == df_spark_memory.source_memory) & (df_spark_batch.target == df_spark_memory.target_memory)

    df_join = df_spark_batch.join(df_spark_memory, condition, how='left').toPandas()

    df_spark_memory.unpersist()

    df_spark_batch.unpersist()

    return df_join.loc[df_join["source_memory"].isna(),["source", "target"]], df_join.loc[~df_join["source_memory"].isna(),["source", "target"]]




def spark_append_to_memory_paths(result_paths_list):

    builder = SparkSession.builder.appName("Spark_Memory_Path") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

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

            df_spark_memory = spark.read.format("delta").load(MEMORY_PATHS_FOLDER)\
            
            df_concatenate = df_spark_memory.unionByName(df_result_spark)
            
            df_concatenate.repartition("source").write.format("delta").mode("overwrite").save(MEMORY_PATHS_FOLDER)

        else:

            print("Overwrite/write results to memory path...")
            
            df_result_spark.repartition("source").write.format("delta").mode("overwrite").save(MEMORY_PATHS_FOLDER)

    else:
        print("Overwrite/write results to memory path...")

        df_result_spark.repartition("source").write.format("delta").mode("overwrite").save(MEMORY_PATHS_FOLDER)

    df_result_spark.unpersist()
     
    



