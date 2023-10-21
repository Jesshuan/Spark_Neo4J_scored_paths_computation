#### ---- HYPERPARAMETERS --------- ####

from pyspark.sql.types import FloatType, IntegerType, DoubleType, StructType,StructField


# Here, all the parameters that could be changed...


# ---- GENERAL parameters ------- #

CONTRIBUTOR = "jesshuan"



# --- Initial FILTERS for cities --- #

# (THe filters before the initial constitution of the source/target lists)

# We can define just one filter for each category (source / destination) in function of one property

# Source cities filtered by :

SOURCE_FILTER_COL = "nb_vp_rechargeables_el"

SOURCE_FILTER_VALUE = 5 # Float value to filter (before this value, cities are removed)

SOURCE_DIVIDE_FACTOR = 0.01 # A factor to limit the memory buffer of the source list in case of "weighted" mode

# Target (destination) cities filtered by :

TARGET_FILTER_COL = "visit"

TARGET_FILTER_VALUE = 2 # Float value to filter

TARGET_DIVIDE_FACTOR = 0.1 # A factor to limit the memory buffer of the target list in case of "weighted" mode

# --------------------------------- #





# --- BATCH parameters --- #

NB_DRAWS = 10000 # Global batch size

NEO4J_BATCH_SIZE = 1000 # Sub-batch size for neo4j request (NEO4J_BATCH must << NB_DRAWS)

RATIO_MIN_DRAWS = 1 # value in [0 , 1]. Only for mode "equiprobable".
# Example : 0.8 means we need at least 0.8 X batch_size tuple of (source / target) not already calculated to validate the batch...
# Otherwise, we compute a new little batch to add to the first batch. Then, a batch of the initial size is formed by truncating the resulting batch.

MAX_LEN_BUFFER_BATCHES_LIST = 10000 # Maximun number of batches stocked in the buffer list and not already processed. Leave a great value here.

# For Fast-Recomputation mode :

BATCH_SIZE_FAST_RECOMPUTATION = 10000 # Batch-size for the "fast recomputation" mode. It could be larger than the initial batch size (NB_DRAWS).

# --------------------------------- #



# --- Final FILTER parameters --- #

# (for the "aggregator" script)

MIN_TRAVELTIME_FILTER = 7200.0 #seconds (2 hours)

# Remove the beginning of each path, in function of this value
# (because we assume that users have charged their vehicle's battery
# at home before setting off and are not going to stop immediately...)

# --------------------------------- #



# --- WEIGHTING PARAMETERS ----- #
# Columns choice, schema, and rules for mode "Equiprobable" --- #

COLUMNS_LIST_FOR_WEIGHTING = ["nb_vp_rechargeables_el", "visit"] # Names of the necessaries features of cities in NEO4J for the next parameters

MAP_COL_NAME_FOR_JOINTURE = {
    "nb_vp_rechargeables_el" : "nb_elec_s", # old_column name : new column name
    "visit" : "visit_t" # You must finish names of columns with "_s" for a source feature and "_t" for a target feature
} 

# YOU MUST CONSERV "insee" in this shema, and add others new columns names :
WEIGHTED_DF_SOURCE_SCHEMA = StructType([\
                        StructField("insee", IntegerType(), True)\
                       ,StructField("nb_elec_s", FloatType(), True)\
                        ])

WEIGHTED_DF_TARGET_SCHEMA = StructType([\
                        StructField("insee", IntegerType(), True)\
                       ,StructField("visit_t", FloatType(), True)\
                        ])

WEIGHTING_RULE = "(1 + nb_elec_s / 100) * (1 + visit_t / 10)" # Rule we use to the weighting during the aggregation


# --------------------------------- #


# --- Intern SPARK parameters ---- #

MAX_ROW_SIZE_PER_TASK = 160 # A value necessary to calculate the number of partitions during a step of spark object generation... 

CLEAN_CACHE_MEMORY_INTERVAL = 10 # Number of batch draws before periodic compaction (of the wemory path file)
                                # and cache cleaning,
                                    # (adapted to the delta spark process)




