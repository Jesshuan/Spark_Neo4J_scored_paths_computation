from variables.hyperparameters import MIN_TRAVELTIME_FILTER

from pyspark.sql import SparkSession

def spark_filter_short_travels(spark_df):

    spark = SparkSession.builder.master("local").getOrCreate()

    return spark_df


    return spark_df.filter(spark_df('totalCost') >= MIN_TRAVELTIME_FILTER)



def spark_filter_cut_begining(spark_df):

    spark = SparkSession.builder.master("local").getOrCreate()

    return spark_df