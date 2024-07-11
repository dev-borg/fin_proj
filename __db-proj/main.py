import logging
import logging.handlers

from sqlalchemy import *
# from sqlalchemy import create_engine, text

from libs.db_connect import *
from libs.log_handler import *
from libs.world_bank import *

from queries.data_structures import *

def main():
    
    # Step 1: Instantiate DataFetcher object to fetch data
    try:
        # fetch_resultquery()        

        # fetch_ndarray()

        # fetch_dataframe()

        # fetch_series()

        fetch_sparse_matrix()
        
    except ValueError as e:
        print(f"Error retrieving data: {str(e)}")
        # Handle the error gracefully, e.g., logging, fallback logic, etc.
        # Raises erro to caller, in this case: CUI (command-line user interface)



    # Dispose of the engine to release resources
    dispose_db_connections()

# Run main()
if __name__ == "__main__":
    main()
