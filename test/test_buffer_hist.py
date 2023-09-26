# %%
from list_management.buffer_and_historical_manager import get_all_historical_batches, read_head_buffer_batches

# %%
data = get_all_historical_batches('new_test_new', 'equiprobable')

print(data)

print("------")
print("------")

# %%
data = read_head_buffer_batches('new_test_new', 'equiprobable')


print(data)



