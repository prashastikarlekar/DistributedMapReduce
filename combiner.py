from google.cloud import storage
import requests
import collections
import os
import ast


def getItem2(key):
    print("---in GET Bucket storage-----", key)
    storage_client = storage.Client.from_service_account_json(
        'prashasti-karlekar-fall2022-9205433610ce.json')
    bucket_name = "mapreduce_storage"
    bucket = storage_client.get_bucket(bucket_name)
    get_newBlob = bucket.get_blob(key)

    print(get_newBlob)
    if get_newBlob.exists():
        with get_newBlob.open('r', encoding="utf-8") as f:
            value = f.readlines()
            # value = f
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


def combine(filename):
    mapped_values = getItem2(filename)
    mapped_values = mapped_values[0]
    mapped_values = ast.literal_eval(mapped_values)
    merged_data = collections.defaultdict(list)
    merged_data = collections.defaultdict(list)
    for key, value in mapped_values:
        merged_data[key].append(value)
    try:
        setItem2("word_count_intermediate.txt",
                 str(merged_data.items()))
        print("WORD COUNT- Intermediate data Written")
        return "Done"
    except Exception as e:
        print("ERROR WHILE SAVING INTERMEDIATE DATA IN BUCKET STORAGE \n", e)
        return "False"


def main(request):
    # /mapper is producing results and accepting params as well
    # if i am returning return ---- from a function is it equivalent to post method
    flag = request.args.get("flag")
    if flag == "combine":
        filename = request.args.get("filename")
        combine(filename)
