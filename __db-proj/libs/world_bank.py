from sqlalchemy.sql import text
import pandas as pd

from libs.log_handler import *
from libs.db_connect import * 

def handle_request():
    # Context Manager with construct
    with get_session() as session:
        try:
            # SQL query 1: GDP Current
            query1 = """
            SELECT * 
            FROM gdp_current
            WHERE Country_Name IN ('United States', 'Canada', 'Argentina', 'Russian Federation', 'Mexico')
            LIMIT 20
            """
            result1 = session.execute(text(query1))
            rows1 = result1.fetchall()
            df1 = pd.DataFrame(rows1, columns=result1.keys())
            logger.info("GDP Current Data:")
            logger.info(df1)
        
            print("GDP Current Data:")
            df1.set_index('Country_Name', inplace=True)
            subset = df1.iloc[:, list(range(4, len(df1.columns)))]
            print(subset)
            print()

            # # Means 
            print("Means:")
            print(subset.mean(axis=0))
            print()

            # Moments
            moments = subset.agg(['mean', 'var', 'std', 'skew', 'kurt'], axis=1)
            # Reset index to move 'Country_Name' to a regular column
            moments.reset_index(inplace=True)
            # # Remove the name of the index (optional step)
            moments = moments.rename_axis(None, axis=1)
            print("Moments:")
            print(moments)
            print()

            # Access row by country name
            argentina_data = moments.loc[moments['Country_Name'] == 'Argentina']
            print(argentina_data)
            print()


            # SQL query 2: GDP Per Capita
            query2 = """
            SELECT Country_Name, `2020`, `2021`, `2022`, `2023`
            FROM gdp_per_capita
            WHERE Country_Name IN ('United States', 'Canada')
            LIMIT 20
            """
            result2 = session.execute(text(query2))
            rows2 = result2.fetchall()
            df2 = pd.DataFrame(rows2, columns=result2.keys())
            logger.info("GDP Per Capita Data:")
            logger.info(df2)

            print("GDP Per Capita Data:")
            print(df2)

            # commit() needed only if writing to database table
            # session.commit()  # Commit the transaction

        except Exception as e:
            logger.setLevel(logging.INFO)  # Set the logging level
            logger.error(f"Error occurred: {str(e)}")
            # rollback() needed only if writing to database table
            session.rollback()  # Rollback the transaction on error
            raise

