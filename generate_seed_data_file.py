import json
import random
import string


def write_to_json_file(data):
    file_path = "data_file.json"

    # Open the file in append mode and write the data as JSON
    with open(file_path, "a") as file:
        json.dump(data, file)
        file.write("\n")

    print(f"Data written to JSON file: {data}")


def generate_data():
    document_name = "".join(random.choice(string.ascii_lowercase) for _ in range(2))
    return {"document": document_name}


if __name__ == "__main__":
    # Specify the total number of entries to write
    total_entries_to_write = 10000

    for _ in range(total_entries_to_write):
        data = generate_data()

        # Instead of sending a POST request, write the data to a JSON file
        write_to_json_file(data)
