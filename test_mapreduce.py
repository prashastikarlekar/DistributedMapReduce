import time
import ast
import collections
import string
from google.cloud import storage
import itertools
import requests
import os
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
    # merged_data = collections.defaultdict(list)
    # for key, value in output:
    #     merged_data[key].append(value)
    #     setItem2("word_count_intermediate.txt",
    #              str(merged_data.items()))
    #     print("WORD COUNT- Intermediate data Written")

    # return merged_data.items()


def combine():
    mapped_values = getItem2("mapped_file.txt")
    mapped_values = mapped_values[0]
    mapped_values = ast.literal_eval(mapped_values)
    merged_data = collections.defaultdict(list)
    for key, value in mapped_values:
        merged_data[key].append(value)

    setItem2("word_count_intermediate.txt",
             str(merged_data.items()))
    print("WORD COUNT- Intermediate data Written")

    return merged_data.items()


def reduce_func(filename):
    combine_out = getItem2(filename)
    combine_out = combine_out[0].strip('dict_items')
    combine_out = ast.literal_eval(combine_out)
    reducer_data = collections.defaultdict(list)
    for key, value in combine_out:
        reducer_data[key].append(sum(value))
    setItem2("word_count_final.txt", str(reducer_data.items()))


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
        map_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/mapper"
        combine_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/combiner"
        headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}

        # input_data = getItem2(inputs)
        # mapper_data = {"inputs": inputs, "flag": "map"}
        mapper_params = dict()
        mapper_params["inputs"] = inputs
        mapper_params["flag"] = "map"

        map_responses = None

    # ==================================WORKING MAPPER SOLUTION==============================

        with Pool(processes=4) as pool:
            try:

                # map_responses = self.pool.map(
                #     map_function, inputs, chunksize=6)
                # map_responses = self.pool.map(
                #     requests.get(map_url, data=mapper_data), chunksize=6)
                # mapper_process = requests.get(map_url, params=mapper_params)
                # map_responses = self.pool.map(mapper_process, inputs, chunksize=6)
                queue = Queue()
                # p = Process(target=requests.get(
                #     map_url, params=mapper_params), args=(inputs, queue))
                p = Process(target=requests.get(
                    map_url, params=mapper_params, headers=headers, verify=False), args=(queue, inputs))
                # p.run()
                p.start()
                p.join()
                # output_mapper = queue.get()

                # setItem2("wc_intermediate.txt", output_mapper)
                p.terminate()

            except Exception as e:

                print("WORD COUNT- MAPREDUCE -- CATCH --" + e)
    # ================================= WORKING MAPPER SOLUTION END=================================
        # mapped_values = getItem2("mapped_file.txt")
        # mapped_values = mapped_values[0]
        # mapped_values = ast.literal_eval(mapped_values)
        combiner_params = dict()
        combiner_params["flag"] = "combine"
        combiner_params["filename"] = "mapped_file.txt"

        # time.sleep(20)
    # =================================WORKING COMBINER SOLUTION======================================
        with Pool(processes=4) as pool:
            try:
                # map_output = itertools.chain(*output_mapper)
                p = Process(target=requests.get(
                    combine_url, params=combiner_params, headers=headers, verify=False), args=("mapped_file.txt",))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in combiner", e)
                return e

    # ===================================WORKING COMBINER SOLUTION END===========================================

        # merged_values = combine(map_responses)
        # combiner_data = {"mapped_values": map_responses, "flag": "combine"}
        # merged_values = requests.get(map_url, data=combiner_data)
        # del map_responses
        reducer_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/reducer"
        reducer_params = dict()
        reducer_params['flag'] = 'reduce'
        reducer_params['filename'] = 'word_count_intermediate.txt'

        with Pool(processes=4) as pool:
            try:
                # map_output = itertools.chain(*output_mapper)
                p = Process(target=requests.get(
                    reducer_url, params=reducer_params, headers=headers, verify=False), args=("word_count_intermediate.txt",))
                # p = Process(target=reduce_func,
                #             args=('word_count_intermediate.txt',))
                p.start()
                p.join()
                # p.run()
            except Exception as e:
                print("Error in reducer", e)
                return e
        # reducer_data = {"merged_values": merged_values, "flag": "reduce"}
        # reduced_values = self.pool.map(requests.get(
        #     reducer_url, data=reducer_data))
        # try:
        #     setItem2(output_location, str(reduced_values))
        #     print("WORD COUNT- Final data Written")
        # except Exception as e:
        #     print("ERROR WHILE SAVING Final DATA IN BUCKET STORAGE \n", e)
        # return True if reduced_values else False
        return 'True'


if __name__ == "__main__":
    res = WordCount(3, 3, 'word_count', 'myFile.txt', 'word-count-final.txt')
    res2 = res('myFile.txt', 'word-count-final.txt')
    # print(res2)
