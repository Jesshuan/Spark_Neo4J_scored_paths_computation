from variables.hyperparameters import MIN_TRAVELTIME_FILTER

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf

from pyspark.sql.types import FloatType, IntegerType, ArrayType


# --- SPARK Filter function --- #


# -- Sub-functions --- #

def binarySearch(array, x, low, high):

    # A reccursive "classical" binary search to find a minimun
    # (or a relative minimun) in a ordered list

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
    
# A spark UDF function used by the main spark function, to truncate the path

@udf(ArrayType(IntegerType()))
def truncate_filter(path_array, costs_array):

    len_arr_c = len(costs_array)

    index_beg = binarySearch(costs_array, MIN_TRAVELTIME_FILTER, 0, len_arr_c - 1)

    return path_array[index_beg:]



def spark_filter_cut_begining(df_spark_batch):

    # --- Get the context ---

    spark = SparkSession.builder.appName("Spark_Filter").master("local").getOrCreate()

    # ----

    # Spark filter :
    # filter 1 : remove the path that are not long enough (MIN_TRAVELTIME_FILTER provided by the user)
    # filter 2 : truncate all the paths with the same value (remove all the nodes of the path list with an associated cost inferior)

    df = df_spark_batch.where(col('totalCost') >= MIN_TRAVELTIME_FILTER)\
                        .withColumn("path_truncated", truncate_filter(col('path'),col("costs")))\
                        .select("source", "target", "path_truncated")\
                        .toPandas()

    return df