from pyspark.sql.functions import col, flatten, collect_list, explode, expr

from pyspark.sql import SparkSession

from variables.hyperparameters import MIN_TRAVELTIME_FILTER

def spark_aggregation(df_spark_batch, mode, weighted_feats_df = None):

    spark = SparkSession.builder.master("local").getOrCreate()

    if mode == "weighted":

        df = df_spark_batch.where(col('totalCost') >= MIN_TRAVELTIME_FILTER)\
            .selectExpr('source','target','inline(arrays_zip(path, costs))')\
            .where(col('costs') >= MIN_TRAVELTIME_FILTER)\
            .toPandas()

# Calculer le produit entre le compte de "collection" et la colonne "weighted"
        #df = df.groupBy("road_point").agg(expr("sum(test_value) as weighted_sum"), expr("count(*) as count"))
        """.groupBy("path").count()\
            .select(col("path").cast("integer"), "count")\
            .withColumnRenamed("path", "insee").withColumnRenamed("count", "value")"""

    return df
'''
    elif mode == "equiprobable":

        return df_spark_batch '''
