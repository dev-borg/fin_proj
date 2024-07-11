"""
Data Utilities Module:

- Fetches data from database into object of type:
    ° ResultProxy
    ° use one of follwoing to access data in rows
        > fetchone()
        > fetchall()

- Transformable to other foramts seperately:
    ° ndarray
    ° series
    ° dataframe
    ° sparse matrix) 

"""

from sqlalchemy.sql import text
from sqlalchemy.engine.cursor import ResultProxy
import numpy as np
import pandas as pd
import scipy as sp 

from libs.db_connect import *


"""
Fetch data from database
Return rows, columns
"""
def fetch_from_database(query):
    with get_session() as session:
        try:
            result = session.execute(text(query)) # Note: returns ResultProxy object (need to fetch from it)
            return result.fetchall(), list(result.keys()) # return rows, columns 
        except Exception as e:
            # print(f"Error fetching data from spreadsheet: {str(e)}")
            raise


"""
Implement logic to read data from a spreadsheet file
Example:
return pd.read_excel('spreadsheet.xlsx')
"""
def read_spreadsheet_file(sheet):
    pass


"""
Implement logic to read data from a generic file (e.g., JSON, XML)
Example:
with open('data.json', 'r') as f:
    return json.load(f)
"""
def read_file_data(file):
    pass


"""
Converts ResultProxy object -> NumPy ndarray
Note: ndarray aka n-dimensional or dense array
"""
def convert_to_ndarray(raw_data):
    try:
        rows, columns = raw_data
        return np.array(rows), columns
    except Exception as e:
        print(f"Error converting ResultQuery object to numpy ndarray - convert_to_ndarray(): {str(e)}")
        raise
    pass


# Converts ResultProxy object -> pandas Series 
def convert_to_series(raw_data, axis, idx):
    rows, cols = raw_data
    try:
        if axis == "row":
            # Fetch index row
            series = pd.Series(rows[idx], index=cols)
        else:
            # Fetch index column
            series = pd.Series([row[idx] for row in rows])
        return series
    except Exception as e:
        print(f"Error converting ResultQuery object to pandas Series convert_to_series(): {str(e)}")
        raise


# Converts ResultProxy object -> pandas DataFrame 
def convert_to_dataframe(raw_data):
    try:
        rows, cols = raw_data
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"Error converting from ResultProxy to pandas Dataframes - convert_to_dataframe(): {e}")
        raise


"""
Converts Data object -> scipy Sparse Matrix
  First:  check for data type
  Second: convert DataFrame (numeric columns only!) then to Sparse Matrix
          Note: conversion to DataFrame makes extracting numeric columns easier
"""
def convert_to_sparse_matrix(raw_data):
    try:
        if not isinstance(raw_data, (list, tuple, pd.DataFrame, np.ndarray)):
            raise TypeError(f"Expected input data to be a tuple, list, DataFrame, or ndarray. Got {type(orig_data)} instead.")
        if isinstance(raw_data, pd.DataFrame):
            # raw_data is DataFrame 
            numeric_cols = raw_data.select_dtypes(include=[np.number]).columns
            data_array = raw_data[numeric_cols].values 
        elif isinstance(raw_data, np.ndarray):
            # raw_data is ndarray
            # convert first to dataframe (easier to work with, cf. numeric columns)
            dataframe = pd.DataFrame(raw_data)
            numeric_cols = dataframe.select_dtypes(include=[np.number]).columns
            data_array = dataframe[numeric_cols].values 
        elif isinstance (raw_data, (list, tuple)):
            # raw_data is list/tuple => ResultQuery type
            dataframe = convert_to_dataframe(raw_data)
            numeric_cols = dataframe.select_dtypes(include=[np.number]).columns
            data_array = dataframe[numeric_cols].values 
        return sp.sparse.csr_matrix(data_array), numeric_cols
    except Exception as e:
        print(f"Error converting from ResultProxy to scipy Sparse Matrix - convert_to_sparse_matrix(): {e}")
        raise
