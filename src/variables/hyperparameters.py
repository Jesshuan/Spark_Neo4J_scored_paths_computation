#### ---- HYPERPARAMETERS --------- ####

from pyspark.sql.types import FloatType, IntegerType, DoubleType, StructType,StructField


# --- Init Filters for Communes --- #

# Source communes filtered by :

SOURCE_FILTER_COL = "nb_vp_rechargeables_el"

SOURCE_FILTER_VALUE = 500.0 # Float value to filter

SOURCE_DIVIDE_FACTOR = 0.01 # A factor to limit the memory buffer of the source list in case of "weighted" mode

# Target communes filtered by :

TARGET_FILTER_COL = "visit"

TARGET_FILTER_VALUE = 200.0 # Float value to filter

TARGET_DIVIDE_FACTOR = 0.1 # A factor to limit the memory buffer of the target list in case of "weighted" mode

# --------------------------------- #





# --- BATCH parameters --- #

NB_DRAWS = 20

NEO4J_BATCH_SIZE = 5

RATIO_MIN_DRAWS = 1

MAX_LEN_BUFFER_BATCHES_LIST = 10000

# For Fast-Recomputation mode :

BATCH_SIZE_FAST_RECOMPUTATION = 27

# --------------------------------- #



# --- FILTERS parameters --- #

MIN_TRAVELTIME_FILTER = 7200.0 #seconds (2 hours)

# --------------------------------- #



# --- Columns choice, schema, and rules for mode "Equiprobable" --- #

COLUMNS_LIST_FOR_WEIGHTING = ["nb_vp_rechargeables_el", "visit"] # Names f the necessaries features of cities in NEO4J 

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

WEIGHTING_RULE = "(1 + nb_elec_s / 100) * (1 + visit_t / 10)"


def weighting_process_rule(source_feat_value, target_feat_value):
    pass

# --------------------------------- #


# --- Intern SPARK parameters ---- #

MAX_ROW_SIZE_PER_TASK = 200


