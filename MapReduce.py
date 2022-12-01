import itertools
import collections
import os
import json
import re
from google.cloud import storage
from multiprocessing import Process, Pool, Queue
import string
import requests

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
        map_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/mapper"
        combine_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/combiner"
        headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}

        mapper_params = dict()
        mapper_params["inputs"] = inputs
        mapper_params["flag"] = "map"

        map_responses = None

    # ==================================WORKING MAPPER SOLUTION==============================

        with Pool(processes=4) as pool:
            try:
                queue = Queue()

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

        with Pool(processes=4) as pool:
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

        with Pool(processes=4) as pool:
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

def split_input(input_location, num_mappers):
    input_file = getItem2(input_location)
    file_size = os.path.getsize(input_file)
    section = file_size/num_mappers + 1
    input_file = (re.sub('[^A-Za-z]+', ' ', input_file))
    (index, split) = (1, 1)

    for char in input_file:
        curr_split_section = str(input_location)+"_chunk_"+str(split-1)
        setItem2(curr_split_section, char)
        if (index > section*split+1) and (char.isspace()):
            split += 1
            curr_split_section = str(input_location)+"_chunk_"+str(split-1)
        index += 1


def merge_files(input_location, num_mappers, num_reducers):
    merged_data = []
    for i in range(0, num_mappers):
        for j in range(0, num_reducers):
            fp = str(input_location)+"_map_file_"+str(i)+"_"+str(j)
            temp_file = getItem2(fp)

            if not os.path.getsize(fp) == 0:
                merged_data.append(json.load(temp_file))
    setItem2(str(input_location)+"_MergedData", merged_data)


def get_keys(input_location):
    temp = str(input_location)+"_MergedData"
    map_response = getItem2(temp)
    output_keys = set()
    for temp_list in map_response:
        for key in temp_list.keys():
            if len(key) > 2:
                output_keys.add(key)
    output_keys = list(output_keys)
    output_keys.sort()
    return output_keys


def join_files(input_location, num_reducers, output_location):
    temp_list = []
    for index in range(0, num_reducers):
        r_file = str(input_location)+"_reduced_file_"+str(index)
        temp_list.append(getItem2(r_file))

    op_dict = {}
    for dict in temp_list:
        op_dict.update(dict)
    setItem2(output_location, op_dict)


class InvertedIndex(object):
    def __init__(self, num_mappers, num_reducers, map_function, input_location, output_location):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.map_func = map_function
        self.input_location = input_location
        self.output_location = output_location

    def execute_map(self, index):
        values = getItem2(str(self.input_location)+"_chunk_"+str(index))
        if self.map_func == "inverted_index":
            # map_res = self.inv_index_mapper(values, index)
            map_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/mapper"
            data = {"values": values, "index": index, "flag": "map_ii"}

            map_res = requests.get(map_url, data=data)
        else:
            map_res = self.map_func(values)

        for r in range(0, self.num_reducers):
            temp = str(self.input_location)+"_map_file_"+str(index)+"_r"
            setItem2(temp, map_res)

    def execute_reduce(self, index):
        output_keys = get_keys(self.input_location)
        chunk_size = (len(output_keys))/self.num_reducers + 1
        start_index = int(chunk_size*(int(index)))
        end_index = int(chunk_size*(int(index)+1))
        temp = output_keys[start_index:end_index]
        result = {}
        for element in temp:
            if self.reduce_func == "inverted_index":
                # data = self.inv_index_reducer(
                #     element, self.input_location, self.num_mappers)
                reduce_url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/reducer"
                reduce_data = {
                    "element": element, "input_location": self.input_location, "num_mappers": self.num_mappers, "flag": "reduce_ii"}
                data = requests.get(reduce_url, data=reduce_data)
            result[element] = data

        temp_file = str(self.input_location)+"_reduced_file_"+str(index)
        setItem2(temp_file, result)

    def execute(self):
        split_input(self.input_location, self.num_mappers)
        map_worker = []
        reducer_worker = []
        restart_map = True
        while restart_map:
            try:
                for process_id in range(self.num_mappers):
                    p = Process(target=self.execute_map, args=(process_id,))
                    p.start()
                    map_worker.append(p)
                [temp.join() for temp in map_worker]
                restart_map = False
            except Exception as E:
                print(E)
        merge_files(self.input_location, self.num_mappers, self.num_reducers)
        restart_red = True
        while restart_red:
            try:
                for process_id in range(self.num_reducers):
                    p = Process(target=self.execute_reduce, args=(process_id,))
                    p.start()
                    reducer_worker.append(p)
                [temp.join() for temp in reducer_worker]
                restart_red = False
            except:
                pass
        join_files(self.input_location, self.num_reducers,
                   self.output_location)
        return True


def main(request):
    url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/master"
    # Accepting requests params
    application = request.args.get("map_function")
    num_mappers = request.args.get("num_mappers")
    num_reducers = request.args.get("num_reducers")
    input_location = request.args.get("input_location")
    output_location = request.args.get("output_location")
    # if application is wc, make an object of WordCount
    if application == "word_count":
        mapped = WordCount(num_mappers, num_reducers,
                           application)
        status = mapped(input_location, output_location)
        if status:
            data = {"status": True}
            requests.post(url, data=data)
            # return True
        else:
            data = {"status": False}
            requests.post(url, data=data)
            return False
    if application == "inverted_index":
        obj = InvertedIndex(num_mappers, num_reducers,
                            application, input_location, output_location)
        status = obj.execute()
        if status:
            data = {"status": True}
            requests.post(url, data=data)
        else:
            data = {"status": False}
            requests.post(url, data=data)
