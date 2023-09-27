import os

import sys

import numpy as np

import time

import pickle

from collections import deque

from variables.memory_path import HISTORICAL_BATCHES_FOLDER, BUFFER_BATCHES_FOLDER

from variables.hyperparameters import MAX_LEN_BUFFER_BATCHES_LIST, BATCH_SIZE_FAST_RECOMPUTATION


def verify_mode(data, mode):
    
    if data['mode']!=mode:

        print("Sorry, but there is an error with the mode provided and the mode of the experiment...")

        sys.exit(2)

    return



def read_a_list(path_file):

    while True:

        try:
        
            with open(path_file, 'rb') as f:

                data = pickle.load(f)

            break

        except EOFError as e:

            print("Waiting until reading... data used by another script....")

            time.sleep(1)

    return data



def initialize_a_list(path_file, batch_list, mode):
     
    data = {"mode": mode,
            "list":batch_list}

    try:
        os.makedirs(HISTORICAL_BATCHES_FOLDER)
    except:
        pass

    while True:

        try:
    
            with open(path_file, 'wb') as f:
         
                pickle.dump(data, f)

            print("New Results written to the historical list.")

            break

        except EOFError as e:

            print("Waiting until reading... data used by another script....")

            time.sleep(1)



def append_to_list(path_file, data, batch_list):

    while True:

        try:

            with open(path_file, 'wb') as f:

                existent_list = data["list"]

                new_list = np.append(existent_list, batch_list, axis=0)

                data['list'] = new_list

                pickle.dump(data, f)

            print("New Results written to the historical list.")

            break

        except EOFError as e:

            print("Waiting until reading... data used by another script....")

            time.sleep(1)


def append_to_queue(path_file, data, batch_list, direction="right"):

    while True:

        try:

            with open(path_file, 'wb') as f:

                existent_queue = data["queue"]

                if direction=='left':
                    existent_queue.appendleft(batch_list)

                else:
                    existent_queue.append(batch_list)

                data['queue'] = existent_queue

                #time.sleep(10)

                pickle.dump(data, f)

            print("New Results added to the buffer list.")

            break

        except EOFError as e:

            print("Waiting until reading... data used by another script....")

            time.sleep(1)

            
def initialize_a_queue(path_file, batch_list, mode):

    queue = deque(maxlen = MAX_LEN_BUFFER_BATCHES_LIST)

    queue.append(batch_list)
     
    data = {"mode": mode,
            "queue":queue}
    
    try:
        os.makedirs(BUFFER_BATCHES_FOLDER)
    except:
        pass

    while True:

        try:
    
            with open(path_file, 'wb') as f:

                #time.sleep(10)
         
                pickle.dump(data, f)

            print("New Results added to the buffer list.")

            break
        
        except EOFError as e:

            print("Waiting until reading... data used by another script....")

            time.sleep(1)



def pop_queue(path_file, data):

    while True:

        try:

            with open(path_file, 'wb') as f:

                queue = data['queue']

                pop_data = queue.popleft()

                data['queue'] = queue

                #time.sleep(5)

                pickle.dump(data, f)

            print("Queue pop !")

            break

        except EOFError as e:

            print("Waiting until reading... data used by another script....")

            time.sleep(1)

    return pop_data






def append_to_historical_batches(batch_list, experiment_name, mode):

    print("Append new batch list to historical batches data.")

    path_file = HISTORICAL_BATCHES_FOLDER + experiment_name + ".pkl"

    if os.path.exists(path_file):

        print("File already here for this experiment")

        data = read_a_list(path_file)

        verify_mode(data, mode)
        
        append_to_list(path_file, data, batch_list)

    else:
         
         print("We generate new historical batches folder for this experiment...")
         
         initialize_a_list(path_file, batch_list, mode)

        

def get_all_historical_batches(experiment_name, mode):

    print("Get all batch list from historical batches data.")

    path_file = HISTORICAL_BATCHES_FOLDER + experiment_name + ".pkl"

    if os.path.exists(path_file):

        data = read_a_list(path_file)

        verify_mode(data, mode)
        
        return data['list']

    else:
         
        print("Folder not found for this experiment...")
         
        return None
    
    

def append_to_buffer_batches(batch_list, experiment_name, mode, direction='right'):

    print("Append new batch list to buffer batches data.")

    path_file = BUFFER_BATCHES_FOLDER + experiment_name + ".pkl"

    if os.path.exists(path_file):

        print("File already here for this experiment")

        data = read_a_list(path_file)

        verify_mode(data, mode)

        if direction=="left":
        
            append_to_queue(path_file, data, batch_list, direction='left')

        else:

            append_to_queue(path_file, data, batch_list)


    else:
        
        print("We generate new buffer batches folder for this experiment...")
         
        initialize_a_queue(path_file, batch_list, mode)



def read_head_buffer_batches(experiment_name, mode):
    
    print("Read the next batch from the buffer batches data...")
    
    path_file = BUFFER_BATCHES_FOLDER + experiment_name + ".pkl"
    
    if os.path.exists(path_file):
        
        data = read_a_list(path_file)
        
        verify_mode(data, mode)

        if len(data['queue']) > 0:
        
            return data['queue'][0]
        
        else:

            return None
    
    else:

        print("Folder not found for this experiment...")
         
        return None
    

def pop_head_buffer_batches(experiment_name, mode):
    
    print("Read the next batch from the buffer batches data...")
    
    path_file = BUFFER_BATCHES_FOLDER + experiment_name + ".pkl"
    
    if os.path.exists(path_file):
        
        data = read_a_list(path_file)
        
        verify_mode(data, mode)
        
        pop_data = pop_queue(path_file, data)

        if len(data['queue']) > 0:
        
            return pop_data
        
        else:

            return None

    else:

        print("Folder not found for this experiment...")
         
        return None
    

def batch_reverse(iterable, n=1):
        l = len(iterable)
        for ndx in range(l, 0, -n):
            yield iterable[max(ndx - n, 0):ndx]


def transfert_historical_to_buffer(experiment_name, exp_fr, mode):

    print("!!!!------------------------!!!!")
    print("Fast-Recomputation Mode :")
    print(f"Transfer : from historical list - experiment : {exp_fr}")
    print(f"---> to buffer batches list - experiment : {experiment_name}")
    print("!!!!------------------------!!!!")

    hist_list = get_all_historical_batches(exp_fr, mode)

    print(f"Founded historical data of length on the experiment : {len(hist_list)} draws ...")

    for hist_batch in batch_reverse(hist_list, BATCH_SIZE_FAST_RECOMPUTATION):

        print(f"Transfert: {len(hist_batch)} draws...")

        append_to_buffer_batches(hist_batch, experiment_name, mode, direction='left')

    print("Transfer done. (Data are now in the left of the buffer batches list) ")
    


    





