from Classes.DataFetcher import *

query = """
SELECT * 
FROM gdp_current
WHERE Country_Name IN ('United States', 'Canada', 'Argentina', 'Russian Federation', 'Mexico')
LIMIT 20
"""

fetcher = DataFetcher("database")

# Print results stored in ResultQuery
def fetch_resultquery():
    try:
        rows, columns = fetcher.get_data_db(query, None, None, None)
    except Exception as e:
        print(f"Error fetching/preprocessing data - fetch_resultquery(): {e}")
        raise

    # Use fetched data
    print("Data fetched and processed successfully")
    print("GDP Current Data:")
    for row in rows:
        print(row)
    print()
    print(columns)
    print()


# Print results stored in ndarray
def fetch_ndarray():
    try:
        rows, columns = fetcher.get_data_db(query, "ndarray", None, None)
    except Exception as e:
        print(f"Error fetching/preprocessing data: {e}")
        raise

    # Use fetched data
    print("Data fetched and processed successfully")
    print("GDP Current Data:")
    for row in rows:
        print(row)
    print()
    print(columns)
    print()

# Print results stored in DataFrame
def fetch_dataframe():
    try:
        data = fetcher.get_data_db(query, "dataframe", None, None)
    except Exception as e:
        print(f"Error fetching/preprocessing data: {e}")
        raise

    # Use fetched data
    print("Data fetched and processed successfully")
    print("GDP Current Data:")
    print(data)
    print()

    # Printing Columns
    print()
    print("Columns:")
    print(data.columns)
    print()
    print("Columns using .tolist()")
    print(data.columns.tolist())
    print()

    # Printing Rows (similar to SQL output stored in ResultQuery object)
    print("Print rows using .itterows():")
    for index, row in data.iterrows():
        print(row.values)
    print()
    
    print("2020 data only:")
    for i in range(0,5):
        print(data['2020'][i])
    print()

    print("2020 data only:")
    print(data['2020'].values)
    print()


# Print results stored in Series 
def fetch_series():
    try:
        data = fetcher.get_data_db(query, "series", "col", 0)

        print("Data fetched and processed successfully")
        print("GDP Current Data:")
        print(data)
    except Exception as e:
        print(f"Error fetching/preprocessing data: {e}")
        raise


# Print results stored in Sparse Matrix object 
def fetch_sparse_matrix():
    try:
        data, cols = fetcher.get_data_db(query, "sparse-matrix", None, None)
        print("Data fetched and processed successfully")
        print("GDP Current Data:")
        print(data)
    except Exception as e:
        print(f"Error fetching/preprocessing data - fetch_sparse_matrix(): {e}")
        raise
    pass

