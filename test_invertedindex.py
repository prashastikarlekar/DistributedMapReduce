import time
import ast
import collections
import string
from google.cloud import storage
import itertools
import requests
import os
import json
from multiprocessing import Process, Pool, Queue


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


def getItem_json(key):
    print("---in GET Bucket storage-----", key)
    storage_client = storage.Client.from_service_account_json(
        'prashasti-karlekar-fall2022-9205433610ce.json')
    bucket_name = "mapreduce_storage"
    bucket = storage_client.get_bucket(bucket_name)
    get_newBlob = bucket.get_blob(key)

    print(get_newBlob)
    if get_newBlob.exists():
        value = json.loads(get_newBlob.download_as_string())
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


def setItem_json(key, value):
    print("In SET bucket storage")

    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'prashasti-karlekar-fall2022-9205433610ce.json'
        storage_client = storage.Client(
            project='prashasti-karlekar-fall2022')
        bucket_name = "mapreduce_storage"
        bucket = storage_client.get_bucket(bucket_name)
        # filename = "bucket_data.json"
        blob = bucket.blob(key)
        blob.upload_from_string(data=json.dumps(
            value), content_type='application/json')
        ret_val = "STORED\r\n"
        return ret_val.encode()
    except Exception as e:
        print(e)
        return "NOT STORED\r\n".encode()


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


def combine_ii(filename):
    mapped_values = getItem2(filename)
    mapped_values = mapped_values[0]
    mapped_values = ast.literal_eval(mapped_values)
    merged_data = collections.defaultdict(list)
    for key, value in mapped_values:
        merged_data[key].append(value)

    setItem2("II_intermediate.txt",
             str(merged_data.items()))
    print("Inverted Index- Intermediate data Written")

    return merged_data.items()


def reduce_func(filename):
    combine_out = getItem2(filename)
    combine_out = combine_out[0].strip('dict_items')
    combine_out = ast.literal_eval(combine_out)
    reducer_data = collections.defaultdict(list)
    for key, value in combine_out:
        reducer_data[json.dumps(key)].append(sum(value))
    # reducer_data = reducer_data[0].strip('dict_items')
    # reducer_data = ast.literal_eval(reducer_data)
    setItem_json("II_final.json", reducer_data)

    # setItem2("II_final.txt", (reducer_data.items()))


class InvertedIndex(object):
    def __init__(self, num_mappers, num_reducers, map_func, input_location, output_location):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.map_func = map_func
        self.input_location = input_location
        self.output_location = output_location
        self.pool = Pool(num_mappers)

        self.url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/master"

    # ------------------------------------KEEP IN MAPREDUCE-EXECUTE----------------------------------------------

    def __call__(self, inputs, output_location, chunksize=6):
        map_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/maper"
        combine_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/combiner"
        headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}

        mapper_params = dict()
        # mapper_params["inputs"] = list(inputs)
        # mapper_params["flag"] = "map_ii"
        for index in range(len(inputs)):
            field = inputs[index]
            mapper_params[f"inputs[{index}]"] = field

        map_responses = None

    # ==================================WORKING MAPPER SOLUTION==============================

        with Pool(processes=4) as pool:
            try:
                # queue = Queue()

                p = Process(target=requests.get(
                    map_url, params=mapper_params, verify=False, headers=headers), args=(inputs,))

                p.start()
                p.join()

                p.terminate()

            except Exception as e:

                print("Inverted index- MAPREDUCE -- CATCH --" + e)

        combiner_params = dict()
        combiner_params["flag"] = "II_combine"
        combiner_params["filename"] = "II_mapped_file.txt"

        with Pool(processes=4) as pool:
            try:

                p = Process(target=requests.get(combine_url, params=combiner_params,
                            headers=headers, verify=False), args=("II_mapped_file.txt",))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in combiner", e)
                return e

        reducer_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/reducer"
        reducer_params = dict()
        reducer_params['flag'] = 'II_reduce'
        reducer_params['filename'] = 'II_intermediate.txt'

        with Pool(processes=4) as pool:
            try:
                p = Process(target=requests.get(reducer_url, params=reducer_params, headers=headers, verify=False), args=(
                    "II_intermediate.txt",))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in reducer", e)
                return e
        # saving final result.json on local system
        try:
            file_data = getItem_json("II_final.json")
            with open("response.json", "w", encoding="utf-8") as f:
                json.dump(file_data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            return e

        return 'True'


if __name__ == "__main__":
    res = InvertedIndex(3, 3, 'inverted-index',
                        ['myFile.txt', 'myFile2.txt'], 'inverted-index-final.txt')
    res2 = res(['myFile.txt', 'myFile2.txt'], 'inverted-index-final.txt')
    # print(res2)
