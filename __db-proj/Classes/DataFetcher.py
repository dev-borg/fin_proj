"""
DataFetcher:
- data_source is one of:
    ° database
    ° spreadsheet
    ° file (json or xml)

- preprocess_data()
    ° if requested, allows conversion from raw data to one of:
        > ndarray
        > series
        > dataframe
        > sparse-matrix 

"""

from libs.data_utils import *

class DataFetcher:
    def __init__(self, data_source):
        if data_source in ["database", "spreadsheet", "file"]:
            self.data_source = data_source
        else:
            raise ValueError("Invalid data source type. Instantiation failed.")

    def data_fetch_db(self, query):
        try:
            # Example: Connect to database and retrieve data
            raw_data = fetch_from_database(query)
            return raw_data
        except Exception as e:
            # print(f"Error fetching data from database: {str(e)}")
            raise

    def data_fetch_spreadsheet(self):
        try:
            # Example: Read data from spreadsheet file
            raw_data = read_spreadsheet_file()
            return raw_data
        except Exception as e:
            # print(f"Error fetching data from spreadsheet: {str(e)}")
            raise

    def data_fetch_file(self):
        try:
            # Example: Read data from file and parse it
            raw_data = read_file_data()
            return raw_data
        except Exception as e:
            # print(f"Error fetching data from file: {str(e)}")
            raise

    """
    Functions for data preprocessing
    Return formats differ by type:
    eg. no-transform->rows, cols; ndarray->rows,cols; dataframe->dataframe
    """
    def preprocess_data(self, raw_data, format, axis, index):
        try:
            if format == "ndarray":
                processed_data = convert_to_ndarray(raw_data) 
            elif format == "series":
                processed_data = convert_to_series(raw_data, axis, index) 
            elif format == "dataframe":
                processed_data = convert_to_dataframe(raw_data) 
            elif format == "sparse-matrix":
                processed_data = convert_to_sparse_matrix(raw_data) 
            return processed_data
        except Exception as e:
            print(f"Error preprocessing data - preprocess_data(): {e}")
            raise

    @staticmethod
    def check_format(format):
        # Test for format validity
        if format not in [None, "ndarray", "series", "dataframe", "sparse-matrix"]:
            raise ValueError("Invalid data format. Data cannot be transformed.")
        

    # Preprocess if requested and return data to get_data_* caller
    def preprocess_if_requested(self, raw_data, format, axis, index):
        if format is None:
            return raw_data
        else:
            try:
                return self.preprocess_data(raw_data, format, axis, index)
            except Exception as e:
                print(f"Error in preprocess request - preprocess_if_requested(): {e}")
                raise

    # Attempt to fetch database data and pre-process if requested
    # Cf. preprocess_if_requested() for return formats by tranformation
    def get_data_db(self, query, format, axis, index):
        try:
            self.check_format(format)
            raw_data = self.data_fetch_db(query)
            return self.preprocess_if_requested(raw_data, format, axis, index)
        except Exception as e:
            print(f"Error retrieving data - get_data_db(): {e}")
            raise


    # Attempt to fetch spreadsheet data and pre-process if requested
    def get_data_spreadsheet(self, sheet, format, axis, index):
        try:
            self.check_format(format)
            raw_data = self.data_fetch_spreadsheet(sheet)
            return self.preprocess_if_requested(raw_data, format, axis, index)
        except Exception as e:
            raise
                
                
    # Attempt to fetch file data and pre-process if requested
    def get_data_file(self, file, format, axis, index):
        try:
            self.check_format(format)
            raw_data = self.data_fetch_file(file) # JSON or XML or delimited
            return self.preprocess_if_requested(raw_data, format, axis, index)
        except Exception as e:
            raise
        