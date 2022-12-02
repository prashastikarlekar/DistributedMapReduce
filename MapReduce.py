import itertools
import collections
import os
import json
import re
from google.cloud import storage
from multiprocessing import Process, Pool, Queue
import string
import requests
from config import config

# ***************************************************************BUCKET CODE START****************************************************************************


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


# ***************************************************************BUCKET CODE END****************************************************************************


# WordCount implementation


class WordCount(object):
    def __init__(self, num_mappers, num_reducers, map_func, input_location, output_location):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.map_func = map_func
        self.input_location = input_location
        self.output_location = output_location
        self.pool = Pool(num_mappers)

        self.url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/master"

# WORD COUNT


# ------------------------------------KEEP IN MAPREDUCE-EXECUTE----------------------------------------------

    def __call__(self, inputs, output_location, chunksize=6):
        map_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/maper"
        combine_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/combiner"
        headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}

        mapper_params = dict()
        mapper_params["inputs"] = inputs
        mapper_params["flag"] = "map"
        queue = Queue()
        mapper_params["queue"] = queue

        map_responses = None

    # ==================================WORKING MAPPER SOLUTION==============================

        with Pool(processes=num_mappers) as pool:
            try:
                # queue = Queue()

                p = Process(target=requests.get(
                    map_url, params=mapper_params, headers=headers, verify=False), args=(queue, inputs))

                p.start()
                p.join()

                p.terminate()

            except Exception as e:

                print("WORD COUNT- MAPREDUCE -- CATCH --" + e)

        combiner_params = dict()
        combiner_params["flag"] = "combine"
        combiner_params["filename"] = "mapped_file.txt"

        with Pool(processes=num_mappers) as pool:
            try:

                p = Process(target=requests.get(
                    combine_url, params=combiner_params, headers=headers, verify=False), args=("mapped_file.txt",))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in combiner", e)
                return e

        reducer_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/reducer"
        reducer_params = dict()
        reducer_params['flag'] = 'reduce'
        reducer_params['filename'] = 'word_count_intermediate.txt'

        with Pool(processes=num_reducers) as pool:
            try:
                p = Process(target=requests.get(
                    reducer_url, params=reducer_params, headers=headers, verify=False), args=("word_count_intermediate.txt",))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in reducer", e)
                return e
        return 'True'


# InvertedIndex implementation

class InvertedIndex(object):
    def __init__(self, num_mappers, num_reducers, map_func, input_location, output_location):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.map_func = map_func
        self.input_location = input_location
        self.output_location = output_location
        self.pool = Pool(num_mappers)


# WORD COUNT

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

        with Pool(processes=num_mappers) as pool:
            try:
                # queue = Queue()

                p = Process(target=requests.get(
                    map_url, params=mapper_params, headers=headers, verify=False))

                p.start()
                p.join()

                p.terminate()

            except Exception as e:

                print("Inverted index- MAPREDUCE -- CATCH --" + e)

        combiner_params = dict()
        combiner_params["flag"] = "II_combine"
        combiner_params["filename"] = "II_mapped_file.txt"

        with Pool(processes=num_mappers) as pool:
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

        with Pool(processes=num_reducers) as pool:
            try:
                p = Process(target=requests.get(reducer_url, params=reducer_params, headers=headers, verify=False), args=(
                    "II_intermediate.txt",))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in reducer", e)
                return e
        return 'True'


def main(num_mappers, num_reducers, map_function, input_location, output_location):
    url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/master"

    # if application is wc, make an object of WordCount
    if map_function == "word_count":
        mapped = WordCount(num_mappers, num_reducers,
                           map_function, input_location, output_location)
        status = mapped(input_location, output_location)
        return status

    if map_function == "inverted_index":
        res = InvertedIndex(num_mappers, num_reducers,
                            map_function, input_location, output_location)
        status = res(input_location, output_location)
        return status


if __name__ == "__main__":
    print(config)
    num_mappers = int(config["num_mappers"])
    num_reducers = int(config["num_reducers"])
    map_function = config["map_function"]
    input_location = config["input_location"]
    output_location = config["output_location"].strip('\n')
    print(num_mappers, type(num_mappers))
    print(num_reducers, type(num_reducers))
    print(map_function, type(map_function))
    print(input_location, type(input_location))
    print(output_location, type(output_location))
    print(main(num_mappers, num_reducers, map_function,
               input_location, output_location))
