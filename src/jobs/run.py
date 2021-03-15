from extract import *
from transform import *
from load import *


"""
run.py
~~~~~~~~~~

This Python module contains an Apache Spark job. 
It consists in extract transform load steps. 
Generate a json formatted graph from files:
    clinical_trials.csv
    drugs.csv
    pubmed.csv
    pubmed.json

"""


def execute_job(spark):

    data_dir_input = "../data/input/"
    data_dir_processed = "../data/processed/"
    data_dir_output = "../data/output/"

    # EXTRACT
    df_trial_from_csv = csv_to_df(spark, data_dir_input + "clinical_trials.csv")
    df_drugs_from_csv = csv_to_df(spark, data_dir_input + "drugs.csv")
    df_pubmed_from_csv = csv_to_df(spark, data_dir_input + "pubmed.csv")
    df_pubmed_from_json = json_to_df(spark, data_dir_input + "pubmed.json")

    # TRANSFORM
    df_nodes_drug, \
    df_nodes_pub_trial, \
    df_nodes_pub_journal, \
    df_nodes_pub_journal, \
    df_relation_pub_trial, \
    df_relation_journal = generate_graph_dfs_from_files(df_pubmed_from_csv,
                                                        df_pubmed_from_json,
                                                        df_trial_from_csv,
                                                        df_drugs_from_csv)

    # LOAD
    save_as_json(df_nodes_drug, data_dir_processed + 'nodes_drug')
    save_as_json(df_nodes_pub_trial, data_dir_processed + 'nodes_journal')
    save_as_json(df_nodes_pub_journal, data_dir_processed + 'nodes_pub_trial')
    save_as_json(df_relation_pub_trial, data_dir_processed + 'relation_journal')
    save_as_json(df_relation_journal, data_dir_processed + 'relation_pub_trial')

    merge_json(data_dir_processed, data_dir_output + "citations_graph.json")
