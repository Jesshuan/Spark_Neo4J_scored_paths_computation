from pyspark.sql.functions import col, flatten, collect_list, explode, expr

from pyspark.sql import SparkSession

from variables.hyperparameters import MAX_ROW_SIZE_PER_TASK,WEIGHTED_DF_TARGET_SCHEMA, WEIGHTED_DF_SOURCE_SCHEMA, MAP_COL_NAME_FOR_JOINTURE, WEIGHTING_RULE



def spark_aggregation(result_paths_list, mode, len_df, weighted_feats_df = None):

    spark = SparkSession.builder.master("local").getOrCreate()

    sc = spark.sparkContext

    print("Transform neo4j results cities path to spark dataframe...")

    RDDmap = sc.parallelize(result_paths_list, numSlices = (len_df // MAX_ROW_SIZE_PER_TASK) + 1)

    df_spark = RDDmap.map(lambda m : (m["source"], \
                                        m["target"], \
                                        m["cities_path"]\
                                        )).toDF(["source", "target", "cities_path"])

    if mode == "weighted":

        print("Spark Aggregration in 'weighted' mode...")

        df = df_spark.withColumn("insee", explode(col("cities_path")))\
            .groupBy("insee").count()\
            .withColumnRenamed("count", "value")\
            .toPandas()

    elif mode== "equiprobable":

        weighted_feats_df.rename(columns = MAP_COL_NAME_FOR_JOINTURE, inplace=True)

        print("Spark Aggregration in 'equiprobable' mode...")

        print("Jointure with the weighted df reference...")

        w_df_s = weighted_feats_df[["insee"] + [col for col in weighted_feats_df.keys() if col.endswith("_s")]]

        w_df_t = weighted_feats_df[["insee"] + [col for col in weighted_feats_df.keys() if col.endswith("_t")]]         

        df_spark_w_s = spark.createDataFrame(w_df_s, schema=WEIGHTED_DF_SOURCE_SCHEMA)

        df_spark_w_t = spark.createDataFrame(w_df_t, schema=WEIGHTED_DF_TARGET_SCHEMA)

        df_join = df_spark.join(df_spark_w_s, (df_spark.source == df_spark_w_s.insee), how='left')\
                        .drop("insee")

        df_join = df_join.join(df_spark_w_t, (df_spark.target == df_spark_w_t.insee), how='left')\
                        .drop("insee")

        print("Aggregation with the provided rule by user...")

        df = df_join.withColumn("insee", explode(col("cities_path")))\
            .withColumn("weighted", expr(WEIGHTING_RULE))\
            .groupBy("insee").agg(expr("sum(weighted) as value"))\
            .toPandas()
        
        df_spark.unpersist()

        df_join.unpersist()

    return df

