# %%
import numpy as np

import time

# %%
from list_management.buffer_and_historical_manager import append_to_buffer_batches, append_to_historical_batches, read_head_buffer_batches, get_all_historical_batches, pop_head_buffer_batches

exp_name = "other_5"

data = read_head_buffer_batches(exp_name, 'equiprobable')

print(f"Read buffer data : {data}")

time.sleep(2)

data = pop_head_buffer_batches(exp_name, 'equiprobable')

print(f"POP buffer data : {data}")

time.sleep(2)

data = read_head_buffer_batches(exp_name, 'equiprobable')

print(f"Read buffer data : {data}")

time.sleep(2)

data = pop_head_buffer_batches(exp_name, 'equiprobable')

print(f"POP buffer data : {data}")


