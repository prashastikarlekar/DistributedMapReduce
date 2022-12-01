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


def reduce_func(combine_out):
    reducer_data = collections.defaultdict(list)
    for key, value in combine_out:
        reducer_data[key].append(sum(value))
    setItem2("word_count_final.txt", str(reducer_data.items()))

 # reduce function for inverted index


def inv_index_reducer(key, input_location, num_mappers):
    temp = str(input_location)+"_MergedData"
    map_res = getItem2(temp)
    temp = []
    for lis in map_res:
        if key in lis.keys():
            temp.append(lis[key])
    result = []
    for i in range(num_mappers):
        count = 0
        for ele in temp:
            if ele[0] == i:
                count += ele[1]
        if count == 0:
            pass
        else:
            result.append([i, count])
    return result


def main(request):
    flag = request.args.get("flag")
    if flag == "reduce":
        item = request.args.get("merged_values")
        reduce_func(item)
    if flag == "reduce_ii":
        element = request.args.get("element")
        input_location = request.args.get("input_location")
        num_mappers = request.args.get("num_mappers")
        inv_index_reducer(element, input_location, num_mappers)
