import os

import pickle

from collections import deque


def append_to_spark_batch_list(PATH_DATA_FOLDER, filename):

    if not os.path.exists(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl"):
        
        buffer_list = deque([], maxlen=10000)

    else:

        with open(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl", 'rb') as f:
    
            buffer_list = pickle.load(f)

    buffer_list.append(filename)


    with open(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl", 'wb') as f:
    
        pickle.dump(buffer_list, f)




def get_spark_batch_list(PATH_DATA_FOLDER):

    if not os.path.exists(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl"):
        
        return "No batch list !"
    
    else:

        with open(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl", 'rb') as f:

            buffer_list = pickle.load(f)

            return buffer_list
        
        

def pop_left_spark_batch_list(PATH_DATA_FOLDER):

    if not os.path.exists(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl"):
        
        return
    
    else:

        with open(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl", 'rb') as f:

            buffer_list = pickle.load(f)

            filename = buffer_list.popleft()

        with open(PATH_DATA_FOLDER + "batchs_to_spark_process.pkl", 'wb') as f:
    
            pickle.dump(buffer_list, f)

        return filename