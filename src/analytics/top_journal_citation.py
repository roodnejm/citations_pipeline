import json
import pandas as pd

"""
top_journal_citation.py
~~~~~~~~~~

Python module providing features to analyze "drugs" json graph

"""

def read_json_graph(file_path):
    """
    Returns list of dictionaries from a json file
    :param file_path: path of the json file
    :return: list of dictionaries
    """
    with open(file_path) as f:

        data = json.load(f)
    return data


def get_top_journal_drugs_cited(data, topn):
    """
    Return top number of journals that cite the most unique drugs.
    If multiple journals have the same number of unique cited drugs,
    the list is sorted by alphabetic order
    :param data:
    :param topn:
    :return:
    """
    journal_nodes = []
    for e in data:
        if "type" in e and e["type"] == "relation"and "journal" in e:
            journal_nodes.append(e)
    df = pd.DataFrame(journal_nodes)
    top_journal = df[['drug', 'journal']]\
        .dropna().drop_duplicates()\
        .groupby("journal").size().reset_index(name='nb drugs')\
        .sort_values(by=['nb drugs','journal'], ascending=[False, True])['journal'].iloc[topn]

    return top_journal


def main():
    """
    Print the top 1 journal that cite the most distinct drugs
    """
    data = read_json_graph("../../data/output/citations_graph.json")
    top_journal = get_top_journal_drugs_cited(data, 1)
    print("Journal that mentions the most unique drugs: ", top_journal)


if __name__ == '__main__':
    main()
