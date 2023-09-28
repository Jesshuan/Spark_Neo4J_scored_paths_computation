from variables.hyperparameters import MIN_TRAVELTIME_FILTER

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf

from pyspark.sql.types import FloatType, IntegerType, ArrayType


def binarySearch(array, x, low, high):

    if high >= low:

        mid = low + (high - low)//2

        # If found at mid, then return it
        if array[mid] == x:
            return mid

        # Search the left half
        elif array[mid] > x:
            return binarySearch(array, x, low, mid-1)

        # Search the right half
        else:
            return binarySearch(array, x, mid + 1, high)

    else:
        return low

@udf(ArrayType(IntegerType()))
def truncate_filter(path_array, costs_array):

    len_arr_c = len(costs_array)

    index_beg = binarySearch(costs_array, MIN_TRAVELTIME_FILTER, 0, len_arr_c - 1)

    return path_array[index_beg:]



def spark_filter_cut_begining(df_spark_batch):

    spark = SparkSession.builder.appName("Spark_Filter").master("local").getOrCreate()

    #truncate_filterUDF = udf(lambda l : truncate_filter(l), ArrayType())

    df = df_spark_batch.where(col('totalCost') >= MIN_TRAVELTIME_FILTER)\
                        .withColumn("path_truncated", truncate_filter(col('path'),col("costs")))\
                        .select("source", "target", "path_truncated")\
                        .toPandas()

    return df