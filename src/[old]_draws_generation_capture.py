
# ---- Imports modules & packages ---

from pyspark.sql import SparkSession, Row


from pyspark.sql.functions import col, flatten, collect_list, explode
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, ArrayType

import pandas as pd

import numpy as np

import datetime

import random

import pickle

from neo4j import GraphDatabase, basic_auth

from neo4j_process.requests import get_all_communes_with_coords, shortest_path, projection_graph

from post_neo4j_process.filter import filter_begining

from database_process.transfert import update_to_database

from spark_process.spark_and_memory_paths import spark_count, compare_to_df_memory, append_to_df_memory

from buffer_lists_process.manage_batch_spark_lists import append_to_spark_batch_list, get_spark_batch_list, pop_left_spark_batch_list

from collections import deque

import time

import os


# --- VARIABLES --- #

# NEO4J Variables

HOST_NEO4J = "bolt://host.docker.internal:7687"

PASSWORD_NEO4J = os.environ["NEO4J_PASSWORD"]

COLUMNS_NEO4J =  ["source",	"target",	"collection",	"totalCost", "costs"]





# Hyperparameters

NB_DRAWS = 50000

BATCH_SIZE_NEO4J = 5000

MINIMAL_TIME = 7200 # 2 hours of autonomy at the beginning of the travel

PARTITION_SIZE_128Mo = 7000


# Data Paths

PATH_DATA_FOLDER = "./../data/paths/"

ROOT_PARQUET_FILE = "draws_paths_batch_"

DF_MEMORY_PATH = "./../data/memory_paths/memory_paths.parquet"


# --- Define date --- #

date_now = datetime.datetime.now()

date_now_not_sparsed = "_".join(str(date_now).split(" ")).split(".")[0].replace(":","-")



# --- NEO4J Connection --- #

driver = GraphDatabase.driver(
  HOST_NEO4J,
  auth=basic_auth("neo4j", PASSWORD_NEO4J))



# Spark session & context
spark = SparkSession.builder.master("local").getOrCreate()
sc = spark.sparkContext

df_shuffle_schema = StructType([ StructField("source", IntegerType(), True)\
                       ,StructField("target", IntegerType(), True)])


df_paths_schema = StructType([ StructField("source", IntegerType(), True),
                               StructField("target", IntegerType(), True),
                               StructField("collection", ArrayType(
                                     StructField("col",IntegerType()))),
                                StructField("totalCost", DoubleType(), True),
                                StructField("costs", ArrayType(
                                     StructField("cos",DoubleType()))) ])



# --- Graph projection --- #

print("Projection graph generation on neo4j...")

graph_proj_name = "proj_map_" + date_now_not_sparsed

with driver.session() as session:
       
    result = session.execute_write(projection_graph, graph_proj_name)

driver.close()

print(f"Graph projection {result[0]['graphName']} generated !...")
      



# --- Récupération des listes de sources et target ---- #

print("Source / target lists uploading...")

with open('./../data/source_target_init_lists/source_list_test.pkl', 'rb') as f:
    source_list = pickle.load(f)

with open('./../data/source_target_init_lists/target_list_test.pkl', 'rb') as f:
    target_list = pickle.load(f)

print("Done.")


# --- Tirages aléatoires sources/target --- #

c = 0

while True: # infinite loop generation of batchs !

    c+=1

    print("--------------------------------------------")
    print("--------------------------------------------")

    print(f"Batch draws generation n°{c}....")

    print(f"Total generation : {c * NB_DRAWS} draws...")

    shuffle_source = [random.choice(source_list) for i in range(NB_DRAWS)]
    shuffle_target = [random.choice(target_list) for i in range(NB_DRAWS)]

    df_shuffle = pd.DataFrame({"source" : shuffle_source, "target" : shuffle_target})

    print("dataframe head : ")

    print(df_shuffle.head())



    if not os.path.exists(DF_MEMORY_PATH):

        df_to_calc = df_shuffle

        df_preprocess = pd.DataFrame(columns=COLUMNS_NEO4J)

        df_empty = pd.DataFrame(columns=COLUMNS_NEO4J)

        mem_count = 0

        time_compare = 0

    else:

        print("Import spark dataframe memory for comparaison...")

        start_time = time.time()

        """df_memory = spark.read.schema(df_paths_schema).parquet(DF_MEMORY_PATH)\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory") """

        df_memory = spark.read.parquet(DF_MEMORY_PATH)\
                            .dropDuplicates(["source", "target"])\
                            .withColumnRenamed("source", "source_memory")\
                            .withColumnRenamed("target", "target_memory")

        df_spark_shuffle = spark.createDataFrame(df_shuffle, schema=df_shuffle_schema)

        df_to_calc, df_preprocess = compare_to_df_memory(df_spark_shuffle, df_memory, COLUMNS_NEO4J)

        mem_count = df_memory.count()

        print(f"Memory spark dataframe have actually {mem_count} rows.")

        df_memory.unpersist()

        time_compare = np.round(time.time() - start_time, 2)

        print(f"Comparaison done in {time_compare} sec. ")

        print("Split on two dataframes :")

        print(f"Dataframe to compute... Length : {len(df_to_calc)}.")
        print(f"Dataframe already computed... Length : {len(df_preprocess)}.")

        print("Data extract : Dataframe to compute")

        print(df_to_calc.head())

        print("Data extract : Dataframe already compute")

        print(df_preprocess.head())

    print("Conversion to records format...")

    props_list = df_to_calc.to_dict('records')

    print(len(props_list))

    

    print("Done.")


    # --- NEO4J shortest paths --- #


    # --- Batch function ---

    def batch(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]



    with driver.session() as session:
        
        results = []

        total_times = []

        #df_result = pd.DataFrame(columns=COLUMNS_NEO4J)
        
        for props_batch in batch(props_list, BATCH_SIZE_NEO4J):
                                    
                                    print(f"Mini-batch neo4j( extract) : {props_batch[0:2]}")
                                    
                                    print("neo4j request...")

                                    start_time = time.time()

                                    result = session.execute_write(shortest_path, props_batch, graph_proj_name)

                                    total_time = time.time() - start_time

                                    total_times.append(total_time)

                                    print(f"batch done in {np.round(total_time/60, 2)} min. ")

                                    results.extend(result)

                                    # - TEST SPARK VERSION -

                                    RDDmap = sc.parallelize(result, numSlices=4)

                                    df_inter = RDDmap.map(lambda m : (m["source"], \
                                                                m["target"], \
                                                                m["collection"], \
                                                                m["totalCost"], \
                                                                m["costs"])).toDF(COLUMNS_NEO4J)


                                    #df_inter = pd.DataFrame(result)

                                    #if df_result.empty:
                                        #df_result = df_inter

                                    #else:

                                        #df_result = pd.concat([df_result, df_inter], axis=0)

                                        #print(type(df_result['collection'].tolist()[0]))

                                        #print(type(df_result['collection'].tolist()[0][0]))

    driver.close()

    print("------------------")

    print(f"All Batch list done on Neo4j in {np.round(sum(total_times)/60, 2)} min. ")

    # --- Processing results in dataframe --- #

    print ("Conversion to spark dataframe... ")

    start_time = time.time()


    RDDmap = sc.parallelize(result) # numSlices= ...

    df_result_spark = RDDmap.map(lambda m : (m["source"], \
                                        m["target"], \
                                        m["collection"], \
                                        m["totalCost"], \
                                        m["costs"])).toDF(COLUMNS_NEO4J)


    #print ("Extract result... ")
    #print(df_result.head())

    nb_part = int(NB_DRAWS/PARTITION_SIZE_128Mo) + 1


    #df_result_spark = spark.createDataFrame(df_result, schema=df_paths_schema)

    print(f"Size estimate : {sc._jvm.org.apache.spark.util.SizeEstimator.estimate(df_result_spark._jdf)}.")
          
    #print("Action count")

    #print(df_result_spark.count())

    print("Conversion done.")
    print(f"Actual nb of partitions : {df_result_spark.rdd.getNumPartitions()}.")

    append_to_df_memory(df_result_spark, mem_count, DF_MEMORY_PATH, nb_part) 


    print("New computations added to spark dataframe memory...")

    #df_result_spark.unpersist()

    time_register = np.round(time.time() - start_time, 2)

    print(f"New Spark dataframe registering in {time_register} sec. ")
    print(f"Total spark time for memory paths : {time_register + time_compare} sec.")


    df_result = df_result_spark.toPandas()
    

    if len(df_preprocess)==0:
        df_total = df_result



    else:
        df_total = pd.concat([df_preprocess, df_result], axis=0)

    print(df_total.info())
    print(df_total.head())


    # Filter 1 : réduction aux paths suffisamment longs

    print("Apply filter 1...")

    paths = df_total.loc[df_total["totalCost"] >= MINIMAL_TIME,:]

    # Filter 2 : suppression du début de chemin et réduction du dataframe


    print("Apply filter 2...")

    paths = filter_begining(paths, MINIMAL_TIME)


    # --- Sauvegarde format parquet ---

    FILE_NAME = PATH_DATA_FOLDER + ROOT_PARQUET_FILE + str(NB_DRAWS) + "_" + date_now_not_sparsed + "_n_" + str(c) + ".parquet"

    paths.to_parquet(FILE_NAME)

    print("Path file save to parquet format.")


    # --- Sauvegarde de la liste des batchs à processer sous spark ---

    append_to_spark_batch_list(PATH_DATA_FOLDER, FILE_NAME)

    print("buffer list saved.")


    # --- Spark process --- #

    print("---- SPARK PROCESS ----")

    print("-------------")

    # Récupération de la buffer list

    buffer_list = get_spark_batch_list(PATH_DATA_FOLDER)

    print(f"Buffer List of batch contains {len(buffer_list)} batchs.")

        
    while (len(buffer_list) >= 1):
        
        filename = buffer_list[0]

        print(f"Spark process for batch {filename}....")

        df_spark = spark.read.parquet(filename)

        print(df_spark.printSchema())
        
        df_result = spark_count(df_spark)

        print(f"Process for this batch done ! Extract of the result :")

        print(df_result.head())

        # --- Updtate to database ---

        update_to_database(df_result)

        print("buffer list saved.")

        pop_left_spark_batch_list(PATH_DATA_FOLDER)

        buffer_list = get_spark_batch_list(PATH_DATA_FOLDER)


       



