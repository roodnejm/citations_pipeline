from utils import *
from pyspark.sql.functions import col, collect_list, explode_outer, lit, lower


"""
transform.py
~~~~~~~~~~

Python module providing utilities to enrich data 
and generate graph structure Dataframes

"""

def generate_graph_dfs(df_drug_citation):
    """
    Generate Dataframes that represent nodes and edges
    of a graph about "drugs", "pubmeds", "clinical_trials" and "journals".
    :param df_drug_citation: source Dataframe
    :return: Dataframes corresponding to nodes and edges
    """
    df_nodes_drug = get_df_nodes_drug(df_drug_citation)
    df_nodes_pub_trial = get_df_node_pub_trial(df_drug_citation)
    df_nodes_pub_journal = get_df_node_pub_journal(df_drug_citation)
    df_relation_pub_trial = get_df_relation_pub_trial(df_drug_citation)
    df_relation_journal = get_df_relation_journal(df_drug_citation)
    return df_nodes_drug, df_nodes_pub_trial, df_nodes_pub_journal, df_relation_pub_trial, df_relation_journal


def generate_citation_source(df_pubmed_csv, df_pubmed_json, df_trial_csv):
    """
    Generate a Dataframe with source type and "title" lowercased.
    """
    df_pubmed = merge_dfs(df_pubmed_csv, df_pubmed_json)
    df_pubmed = add_source_type(df_pubmed, "pubmed")

    # add source type and merge with pubmeds
    df_trial = rename_column(df_trial_csv, "scientific_title", "title")
    df_trial = add_source_type(df_trial, "clinical trial")

    df = df_pubmed.select('id', 'title', 'date', 'journal', 'source_type')\
        .union(df_trial.select('id', 'title', 'date', 'journal', 'source_type'))

    # split and create a column with title as list of string + lower case
    df_pubtrial_lower = df.withColumn("title_lower", lower(col('title')))
    return df_pubtrial_lower


def generate_drug_citation_df(df_drugs_from_csv, df_pubtrial):
    """
    Generate a Dataframe containing "drugs" and source citation info.
    """
    # lowercase drugs name
    df_drugs_lower = df_drugs_from_csv.withColumn('drug_lower', lower(col('drug')))

    # use a regex to filter list of tokens in citations dataframe
    drugs_list = get_col_value_list(df_drugs_lower, "drug_lower")

    df_article_drugs = add_tokens_as_col(df_pubtrial, "cited_drugs", drugs_list)
    df_drug_citation = get_drug_citation_df(df_article_drugs, "cited_drugs", df_drugs_lower)

    return df_drug_citation


def generate_graph_dfs_from_files(df_pubmed_csv, df_pubmed_json, df_trial_csv, df_drugs_csv):
    """
    Run several processing to enrich data and
    generate multiple Dataframes that represent a graph (nodes and edges).
    Execute se
    :param df_pubmed_csv:
    :param df_pubmed_json:
    :param df_trial_csv:
    :param df_drugs_csv:
    :return:
    """
    df_pubtrial_lower = generate_citation_source(df_pubmed_csv, df_pubmed_json, df_trial_csv)

    df_drug_citation = generate_drug_citation_df(df_drugs_csv, df_pubtrial_lower)

    # generate pool of nodes and relations as individual Dataframe
    df_nodes_drug, df_nodes_pub_trial, df_nodes_pub_journal, df_relation_pub_trial, \
     df_relation_journal = generate_graph_dfs(df_drug_citation)

    return df_nodes_drug, df_nodes_pub_trial, df_nodes_pub_journal, df_nodes_pub_journal, df_relation_pub_trial, \
        df_relation_journal


def merge_dfs(df1, df2):
    """
    Merge two Dataframes and select specific fields.
    """
    df = df1.select('id', 'title', 'date', 'journal')\
        .union(df2.select('id', 'title', 'date', 'journal'))

    return df


def add_source_type(df, source_type):
    """
    Add a column source_type set with a literal or overwrite it if exists.
    """
    return df.withColumn("source_type", lit(source_type))


def rename_column(df, source_col, target_col):
    """
    Rename a column 'source_col' as 'target_col' name.
    """
    df_renamed = df.withColumnRenamed(source_col, target_col)
    return df_renamed


def get_col_value_list(df, col_name):
    """
    Return a list of unique values from a Dataframe column.
    The list is not parallelize.
    """
    # create a list of drugs name tokens in lower case
    values_list = [row[0] for row in df.select((col(col_name))).distinct().collect()]

    return values_list


def add_tokens_as_col(df, col_name, values_list):
    """
    Add a column to the Dataframe containing the list of extracted tokens
    based on the value list provided.
    :param df: the source dataframe
    :param col_name: the column where to search for tokens
    :param values_list: the list of tokens that match in the text column
    :return: A dataframe with a new tokens list column added
    """
    df_filtered_tokens = df.withColumn('cited_drugs',
                    udf_regexp_extract_all('title_lower',
                                           lit('((?<=^)|(?<=\s))(' + '|'.join(values_list) + ')(?=,|\s|$)')))
    return df_filtered_tokens

def get_drug_citation_df(df, col_name, df_join):
    """
    Returns a Dataframe with drug description
    for each publication or trial (each row)
    that mentioned the drug
    :param df:
    :param col_name:
    :param df_join:
    :return: a Dataframe for each citation
    """
    # explode dataframe by remaining tokens (drug name)
    df_article_drug = df.withColumn("drug_lower", explode_outer(col(col_name))).drop(col_name)

    # join on drugs dataframe
    df_drug_citation = df_join.join(df_article_drug, how='full', on='drug_lower')
    return df_drug_citation


def get_df_nodes_drug(df):
    """
    Get graph nodes Dataframe "drugs"
    :param df:
    :return:
    """
    # drug + reference id clinical and pubmed
    df_nodes_drug = df.select("drug","atccode","id").groupBy("drug","atccode").agg(collect_list("id"))\
        .withColumn("type", lit("node")).select("drug","atccode")
    return df_nodes_drug


def get_df_node_pub_trial(df):
    """
    Get graph nodes Dataframe "pubmed" and "clinical trial"
    :param df:
    :return:
    """
    # id -> clinical / pubmed / journals
    df_nodes_pubmed_clinical = df.select("id","source_type","title").withColumn("type", lit("node"))
    return df_nodes_pubmed_clinical


def get_df_node_pub_journal(df):
    """
    Get graph nodes Dataframe "journal"
    :param df:
    :return:
    """
    # group by drugs, journal, date (with id etc)
    df_nodes_journal = df.select("journal").withColumn("source_type", lit("journal"))\
        .withColumn("title", col("journal")).withColumn("type", lit("node"))
    return df_nodes_journal


def get_df_relation_journal(df):
    """
    Get graph edges Dataframe "drugs" to "journal"
    :param df:
    :return:
    """
    # drug -> date -> id ou journal name
    df_relation_journal = df.select("drug", "journal", "date").distinct().withColumn("type", lit("relation"))
    return df_relation_journal


def get_df_relation_pub_trial(df):
    """
    Get graph edges Dataframe "drugs" to "pubmed" and "clinical trial"
    :param df:
    :return:
    """
    df_relation_pub_trial = df.select("drug", "id", "date").distinct().withColumn("type", lit("relation"))
    return df_relation_pub_trial

