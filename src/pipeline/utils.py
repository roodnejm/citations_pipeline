import re
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf


"""
utils.py
~~~~~~~~
Module containing helper functions useful to jobs
"""

def regexp_extract_all(s, pattern):
    """
    Return all matching patterns found in s as a list
    (empty if no match)
    :param s: a String where to search for matches
    :param pattern: a regular expression
    :return: a list with found matches
    """
    pattern = re.compile(pattern, re.M)
    all_matches = re.findall(pattern, s)
    if len(all_matches) == 1:
        return [all_matches[0][1]]
    elif len(all_matches) > 1:
        return list(set(filter(None, [val for sublist in all_matches for val in sublist])))
    else:
        return all_matches


# parallelize udf version of regexp_extract_all
udf_regexp_extract_all = udf(regexp_extract_all, ArrayType(StringType()))
