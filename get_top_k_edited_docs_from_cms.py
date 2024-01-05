import json
import sqlite3

from probables import CountMinSketch, HeavyHitters


def combine_count_min_sketches(sketches):
    result_sketch = sketches[0]
    for sketch in sketches[1:]:
        result_sketch.join(sketch)
    print(f"result_sketch = {result_sketch}")
    return result_sketch


def combine_keys(keys_list):
    final_keys = set()
    for keys in keys_list:
        for key in keys:
            final_keys.add(key)
    return final_keys


def read_count_min_sketches_from_db(db_path):
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Fetch serialized data from the database
    cursor.execute(
        """
        SELECT logger_countminsketchkeys.keys, logger_countminsketchmodel.serialized_data
        FROM logger_countminsketchkeys
        JOIN logger_countminsketchmodel
        ON logger_countminsketchkeys.count_min_sketch_id = logger_countminsketchmodel.id;
    """
    )

    key_and_cms_list = cursor.fetchall()

    # Process the results as needed
    processed_data = [
        (CountMinSketch.frombytes(serialized_data), json.loads(keys))
        for keys, serialized_data in key_and_cms_list
    ]

    connection.close()
    return processed_data


# Function to display top k elements from the combined CountMinSketch
def display_top_k_elements(cms: CountMinSketch, keys: set, k):
    hh = HeavyHitters(10, width=cms.width, depth=cms.depth)
    for key in keys:
        hh.add(key, cms.check(key))
    print(hh.heavy_hitters)
    return hh.heavy_hitters


# Set your SQLite database file path
db_path = (
    "/Users/mohit/My_Work/ArteriaAI/Top_K_distributed_demo/LoggingService/db.sqlite3"
)

# Set the value of k for top k elements
k = 10

# Read CountMinSketches from the database
cms_and_keys = read_count_min_sketches_from_db(db_path)

# Combine CountMinSketches
combined_sketch = combine_count_min_sketches([cms_keys[0] for cms_keys in cms_and_keys])

combined_keys = combine_keys([cms_keys[1] for cms_keys in cms_and_keys])

# Display top k elements from the combined CountMinSketch
display_top_k_elements(combined_sketch, combined_keys, k)
