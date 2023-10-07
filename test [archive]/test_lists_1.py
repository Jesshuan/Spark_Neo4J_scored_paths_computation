
import numpy as np

import time


from list_management.buffer_and_historical_manager import append_to_buffer_batches, append_to_historical_batches, read_head_buffer_batches, get_all_historical_batches, pop_head_buffer_batches


batch_list_1 = np.array([[1, 2],[3, 4]])

batch_list_2 = np.array([[5, 6],[7, 8]])

batch_list_3 = np.array([[9, 10],[11, 12]])

exp_name = "other_5"

print(f" add to the buffer list {batch_list_1}")
append_to_buffer_batches(batch_list_1, exp_name, 'equiprobable')

time.sleep(4)

print(f" add to the buffer list {batch_list_2}")
append_to_buffer_batches(batch_list_2, exp_name, 'equiprobable')

time.sleep(3)

print(f" add to the buffer list {batch_list_3}")
append_to_buffer_batches(batch_list_3, exp_name, 'equiprobable')

