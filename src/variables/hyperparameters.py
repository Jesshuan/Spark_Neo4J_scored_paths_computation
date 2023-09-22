#### ---- HYPERPARAMETERS --------- ####

# --- Init Filters for Communes --- #

# Source communes filtered by :

SOURCE_FILTER_COL = "nb_vp_rechargeables_el"

SOURCE_FILTER_VALUE = 10.0 # Float value to filter

SOURCE_DIVIDE_FACTOR = 0.01 # A factor to limit the memory buffer of the source list in case of "weighted" mode

# Target communes filtered by :

TARGET_FILTER_COL = "visit"

TARGET_FILTER_VALUE = 2.0 # Float value to filter

TARGET_DIVIDE_FACTOR = 0.1 # A factor to limit the memory buffer of the target list in case of "weighted" mode


# --- BATCH parameters --- #

NB_DRAWS = 20

NEO4J_BATCH_SIZE = 5


