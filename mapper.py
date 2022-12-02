
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

# ------------------------WORD COUNT MAPPER------------------------------
def map_function(queue, filename):   # map function for word count
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
        queue.put(output)
        print("Starting Group By")
        # return output

    except Exception as e:
        return 'False from mapper'
# ================================WORD COUNT MAPPER END================================


# ================================INVERTED INDEX MAPPER================================
def map_function_ii(filename):   # map function for word count
    output = []
    STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with', 'on'
    ])

    TR = str.maketrans(
        string.punctuation, ' ' * len(string.punctuation))
    for file in filename:
        f = getItem2(file)
        for line in f:
            line = line.translate(TR)  # Strip punctuation
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS and len(word) > 1:
                    output.append(((word, file), 1))
    try:
        # output = output[1:-1].split(',')
        setItem2("II_mapped_file.txt", output)
        print("Mapping done")
        print("Starting Group By")
        return "Done"

    except Exception as e:
        return 'False from mapper'
# ================================INVERTED INDEX MAPPER END================================


# -------------------------MAIN------------------------------------------------


def main(request):
    # /mapper is producing results and accepting params as well
    # if i am returning return ---- from a function is it equivalent to post method
    flag = request.args.get("flag")
    if flag == "map":
        filename = request.args.get("inputs")
        map_function(filename)

    if flag == "map_ii":
        inputs = request.args.get("inputs")
        map_function_ii(inputs)
