
import requests
import collections
from google.cloud import storage
import json
import os
import string


# ------------------------BUCKET STORAGE----------------------------------

def getItem2(key):
    print("---in GET Bucket storage")
    storage_client = storage.Client.from_service_account_json(
        'prashasti-karlekar-fall2022-9205433610ce.json')
    bucket_name = "mapreduce_storage"
    bucket = storage_client.get_bucket(bucket_name)
    get_newBlob = bucket.get_blob(key)

    print(get_newBlob)
    if get_newBlob.exists():
        with get_newBlob.open('r', encoding="utf-8") as f:
            value = f.readlines()
    else:
        value = "OBJECT NOT FOUND"
    return value


def setItem2(key, value):
    print("In SET bucket storage")

    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'prashasti-karlekar-fall2022-9205433610ce.json'
        storage_client = storage.Client(
            project='prashasti-karlekar-fall2022')
        bucket_name = "mapreduce_storage"
        bucket = storage_client.get_bucket(bucket_name)
        # filename = "bucket_data.json"
        blob = bucket.blob(key)
        blob.upload_from_string(str(value))
        ret_val = "STORED\r\n"
        return ret_val.encode()
    except Exception as e:
        print(e)
        return "NOT STORED\r\n".encode()


# ------------------------BUCKET END--------------------------------------

# ------------------------WORD COUNT MAPPERS------------------------------
def map_function(filename):   # map function for word count
    output = []
    STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with', 'on'
    ])

    TR = str.maketrans(
        string.punctuation, ' ' * len(string.punctuation))
    f = getItem2(filename)
    for line in f:
        line = line.translate(TR)  # Strip punctuation
        for word in line.split():
            word = word.lower()
            if word.isalpha() and word not in STOP_WORDS and len(word) > 1:
                output.append((word, 1))
    try:
        # output = output[1:-1].split(',')
        setItem2("mapped_file.txt", output)
        print("Mapping done")

        return output
    except Exception as e:
        return 'False from mapper'
        print(e)


def combine(mapped_values):
    merged_data = collections.defaultdict(list)
    for key, value in mapped_values:
        merged_data[key].append(value)
    try:
        setItem2("word_count_intermediate.txt",
                 str(merged_data.items()))
        print("WORD COUNT- Intermediate data Written")
    except Exception as e:
        print("ERROR WHILE SAVING INTERMEDIATE DATA IN BUCKET STORAGE \n", e)

    return "True"


def inv_index_mapper(value, index):
    result = {}
    words = value.split()
    for word in words:
        temp_list = [index, 1]
        word = word.lower()
        if word in result.keys():
            result[word][1] += 1
        else:
            result[word] = temp_list
    return result

# -------------------------MAIN------------------------------------------------


def main(request):
    # /mapper is producing results and accepting params as well
    # if i am returning return ---- from a function is it equivalent to post method
    flag = request.args.get("flag")
    if flag == "map":
        filename = request.args.get("inputs")
        map_function(filename)
    if flag == "combine":
        mapped_values = request.args.get("mapped_values")
        combine(mapped_values)

    if flag == "map_ii":
        values = request.args.get("values")
        index = request.args.get("index")
        inv_index_mapper(values, index)
