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


def combine(mapped_values):
    merged_data = collections.defaultdict(list)
    for key, value in mapped_values:
        merged_data[key].append(value)

    setItem2("word_count_intermediate.txt",
             str(merged_data.items()))
    print("WORD COUNT- Intermediate data Written")

    return merged_data.items()


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
                p = Process(target=map_function, args=(queue, inputs))
                # p.run()
                p.start()
                output_mapper = queue.get()
                p.join()
                # setItem2("wc_intermediate.txt", output_mapper)

            except Exception as e:

                print("WORD COUNT- MAPREDUCE -- CATCH --" + e)
    # ================================= WORKING MAPPER SOLUTION END=================================

    # =================================WORKING COMBINER SOLUTION======================================
        with Pool(processes=4) as pool:
            try:
                # map_output = itertools.chain(*output_mapper)
                p = Process(target=combine, args=(output_mapper,))
                p.start()
                p.join()
            except Exception as e:
                print("Error in combiner", e)
    # ===================================WORKING COMBINER SOLUTION END===========================================

        # merged_values = combine(map_responses)
        # combiner_data = {"mapped_values": map_responses, "flag": "combine"}
        # merged_values = requests.get(map_url, data=combiner_data)
        # del map_responses
        # reducer_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/reducer"
        # reducer_data = {"merged_values": merged_values, "flag": "reduce"}
        # reduced_values = self.pool.map(requests.get(
        #     reducer_url, data=reducer_data))
        # try:
        #     setItem2(output_location, str(reduced_values))
        #     print("WORD COUNT- Final data Written")
        # except Exception as e:
        #     print("ERROR WHILE SAVING Final DATA IN BUCKET STORAGE \n", e)
        # return True if reduced_values else False
if __name__ == "__main__":
    res = WordCount(3, 3, 'word_count', 'myFile.txt', 'word-count-final.txt')
    res2 = res('myFile.txt', 'word-count-final.txt')
    print(res2)
