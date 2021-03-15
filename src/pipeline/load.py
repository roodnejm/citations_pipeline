import json
import glob


"""
load.py
~~~~~~~~~~

Python module providing utilities to save Dataframe as json files

"""


def save_as_json(df, path):
    """
    Save Dataframe as a unique json file
    :param df: source Dataframe
    :param path: output path
    :return: None
    """
    df.coalesce(1).write.format('json').mode('overwrite').save(path)


def merge_json(dir, out_filepath):
    """
    Merge all json files from a directory and save result as a unique json file
    :param dir: path directory where are json files to merge
    :param out_filepath: output directory
    :return: None
    """
    result = []
    print(dir + "*/*.json")
    for f in glob.glob(dir + "*/*.json"):
        print("filename: ", f)
        with open(f, "rb") as infile:
            for line in infile:
                print("line: ", line)
                result.append(json.loads(line))
    print(result)

    with open(out_filepath, "w", encoding="utf8") as outfile:
        json.dump(result, outfile)