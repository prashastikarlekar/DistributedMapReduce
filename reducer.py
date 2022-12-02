import ast
import requests
import json
import os
from google.cloud import storage
import collections
# -------------------------BUCKET STORAGE -GET -------------------------------


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

# reduce function for word count


def reduce_func(filename):
    combine_out = getItem2(filename)
    combine_out = combine_out[0].strip('dict_items')
    combine_out = ast.literal_eval(combine_out)
    reducer_data = collections.defaultdict(list)
    for key, value in combine_out:
        reducer_data[key].append(sum(value))
    setItem2("word_count_final.txt", str(reducer_data.items()))
    return "Done"

 # reduce function for inverted index


def reduce_func_ii(filename):
    combine_out = getItem2(filename)
    combine_out = combine_out[0].strip('dict_items')
    combine_out = ast.literal_eval(combine_out)
    reducer_data = collections.defaultdict(list)
    for key, value in combine_out:
        reducer_data[key].append(sum(value))
    setItem2("II_final.txt", str(reducer_data.items()))
    return "Done"


def main(request):
    flag = request.args.get("flag")
    if flag == "reduce":
        filename = request.args.get("filename")
        reduce_func(filename)
    if flag == "II_reduce":
        filename = request.args.get("filename")
        reduce_func_ii(filename)
