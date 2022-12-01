import requests


def main(num_mappers, num_reducers, map_function, input_location, output_location):
    url = "https://us-central1-prashasti-karlekar-fall2022.cloudfunctions.net/MapReduce"
    data = {"num_mappers": num_mappers,
            "num_reducers": num_reducers, "map_function": map_function, "input_location": input_location, "output_location": output_location}
    if map_function == "word_count":
        try:
            status = requests.get(url, data=data)

            if status.text == 'True':
                res = "Task completed."
            else:
                res = "Task NOT completed"
            return res
        except Exception as e:
            return e

    elif map_function == "inverted_index":
        try:
            obj = requests.get(url, data=data)
            if status:
                res = "Task completed."

            return res
        except Exception as e:
            return e


if __name__ == "__main__":
    with open("config.txt", 'r', encoding="utf-8") as myfile:
        res = {}
        config = myfile.read()
        pairs = config.split(",")
        for p in pairs:
            key, value = p.split("=")
            res[key] = value
        print(res)
    myfile.close()
    num_mappers = int(res["num_mappers"])
    num_reducers = int(res["num_reducers"])
    map_function = res["map_function"]
    input_location = res["input_location"]
    output_location = res["output_location"]
    # print(num_mappers, type(num_mappers))
    # print(num_reducers, type(num_reducers))
    # print(map_function, type(map_function))
    # print(input_location, type(input_location))
    # print(output_location, type(output_location))
    print(main(num_mappers, num_reducers, map_function,
               input_location, output_location))
