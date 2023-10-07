
from pyspark.sql.functions import col, flatten, collect_list, explode, broadcast

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, ArrayType

from pyspark.sql import SparkSession

from variables.memory_path import MEMORY_PATHS_FOLDER

from variables.hyperparameters import NB_DRAWS, MAX_ROW_SIZE_PER_TASK

import os

from delta import *


# --- Spark functions ---- #
# --- In relation with the memory path file ---- #


def spark_get_delta_table_from_memory_paths():

    # A function to return the spark delta table used during all the script 
    # Very practice to get the variable as an "in-memory" variable, in case of updates...

    # - Get the spark context -

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    # ---

    delta_table = DeltaTable.forPath(spark, MEMORY_PATHS_FOLDER)

    return delta_table


def spark_get_all_memory_paths():

    # A function just for test. Read the parquet file "memory path" from the disk (not "in-memory").

    # - Get the spark context -

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    # ---

    df_spark_memory = spark.read.format("delta").load(MEMORY_PATHS_FOLDER)\
                            .dropDuplicates(["source", "target"])
    
    return df_spark_memory



def spark_get_a_df_memory_paths_selection_from_batch(batch_list):

    # A function used by the "aggregator" script.
    # The program provide a batch list of tuple (source / target)... and this batch is converted to a spark dataframe.
    # A join with the memory path spark dataframe (read here from the disk, not the delta table) allows us to
    # retain all the calculated paths (paths of road points nodes).
    # This new complete dataframe is return as a pandas dataframe (it's then easier to make a neo4J request...)

    # - Get the spark context -

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

     # ----

    df_shuffle_schema = StructType([ StructField("source", IntegerType(), True)\
                       ,StructField("target", IntegerType(), True)])
    
    df_spark_batch = spark.createDataFrame(batch_list, schema=df_shuffle_schema)

    df_spark_memory = spark.read.format("delta").load(MEMORY_PATHS_FOLDER)\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory")
    
    # - JOIN -

    condition = (df_spark_batch.source == df_spark_memory.source_memory) & (df_spark_batch.target == df_spark_memory.target_memory)

    df_join = df_spark_batch.join(df_spark_memory, condition, how='left').select(["source","target","path","totalCost","costs"])

    # -- Clean cache of the spark in-memory --

    df_spark_memory.unpersist()

    df_spark_batch.unpersist()

    return df_join


def spark_comparison_to_df_memory_paths(df_batch, delta_table):

    # Function used by the "path calculator" script.
    # A function to compare a batch (dataframe with (source, target) tuple provided)
    # with all the tuple (source / target) already calculated and stored in the "memory paths" file
    # First, we convert the pandas dataframe in a spark dataframe
    # After that, a join is made with the memory paths spark dataframe.
    # We split the result in two parts (presence or not of paths already calculated)
    # The result is a pandas dataframe

    # - Get the spark context -

    builder = SparkSession.builder.appName("Spark_Memory_Path")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    # ----

    df_shuffle_schema = StructType([ StructField("source", IntegerType(), True)\
                       ,StructField("target", IntegerType(), True)])

    df_spark_batch = spark.createDataFrame(df_batch, schema=df_shuffle_schema)
    
    df_spark_memory = delta_table.toDF().select("source", "target")\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory")

    #-- JOIN --

    condition = (df_spark_batch.source == df_spark_memory.source_memory) & (df_spark_batch.target == df_spark_memory.target_memory)

    df_join = df_spark_batch.join(df_spark_memory, condition, how='left').toPandas()

    # -- Clean cache of the spark in-memory --

    df_spark_memory.unpersist()

    df_spark_batch.unpersist()

    return df_join.loc[df_join["source_memory"].isna(),["source", "target"]], df_join.loc[~df_join["source_memory"].isna(),["source", "target"]]




def spark_append_to_memory_paths(result_paths_list, delta_table):

    # Function used by the "path calculator" script.
    # This function store a new batch of calculated path (as a big list) in the memory path file. (delta spark format)
    # If no memory paths file exists... it's an overwritting.
    # In others case it's an "append"...

    # - Get the spark context -

    builder = SparkSession.builder.appName("Spark_Memory_Path") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    sc = spark.sparkContext

    # ----

    # Convert the results list in a rdd object, then in a spark dataframe

    # We use the parallelize method... with the option numSlices with a fixed number of partitions to prevent in-memory saturation.

    RDDmap = sc.parallelize(result_paths_list, numSlices = (NB_DRAWS // MAX_ROW_SIZE_PER_TASK) + 1)

    df_result_spark = RDDmap.map(lambda m : (m["source"], \
                                        m["target"], \
                                        m["path"], \
                                        m["totalCost"], \
                                        m["costs"])).toDF(["source","target","path","totalCost","costs"])
    
    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            # "Append" mode :

            print("Append results to memory path...")

            # A jointure... in delta spark format
            # Delta spark format add a new partition with the new data
            # and verify no matching with the old version of files...

            condition = "existing.source = new.source AND existing.target = new.target"

            delta_table.alias("existing").merge(df_result_spark.alias("new"), condition) \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        else:

            # "Overwriting" mode : 
            # We must use the ".repartition()" method to define automatically a good number of partitions,
            # without this method, the "num slice" previously provided in the RDD object conversion will make
            # a too big generation of partitions, when writing the file.

            print("Overwrite/write results to memory path...")
            
            df_result_spark.repartition().write.format("delta").mode("overwrite").save(MEMORY_PATHS_FOLDER)

    else:
        print("Overwrite/write results to memory path...")

        df_result_spark.repartition().write.format("delta").mode("overwrite").save(MEMORY_PATHS_FOLDER)

    # Spark cache cleaning :

    df_result_spark.unpersist()


def spark_recompact_file():

    # -- A re-compaction function ---
    # Periodically, this function is used by the " path calculator" script, just after a new writing,
    # to reduce the number of partitions.
    # Indeed, the delta spark storage mode generate a new little partition for each "append"...
    # We therefore need to compact the file, from time to time,
    # into an adequate number of partitions of balanced size.
    # (The period is supplied by the user (default: 10 writes))

     # - Get the spark context -

    builder = SparkSession.builder.appName("Spark_Memory_Path") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).master("local").getOrCreate()

    # ---

    # Compaction is just a simple read followed by a simple write
    # (option "dataChange" == false, to ensure we don't alterate the data...)
    
    spark.read.format("delta").load(MEMORY_PATHS_FOLDER) \
        .write.option("dataChange", "false").format("delta").mode("overwrite").save(MEMORY_PATHS_FOLDER)

     
    



