import pandas as pd
import numpy as np
import psycopg2

def filter_and_copy_to_postgres(input_dataframe, table_name, columns):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="8252",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Ensure the DataFrame only contains the specified columns
        df = input_dataframe[columns]

        # Replace empty strings with NaN values
        df.replace('', np.nan, inplace=True)

        # Drop rows with missing values
        df.dropna(subset=['latitude', 'longitude'], inplace=True)

        # Use COPY command to insert data into table
        from io import StringIO
        output = StringIO()
        df.to_csv(output, sep=',', header=False, index=False)
        output.seek(0)

        cursor.copy_from(output, table_name, sep=',')

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data:", error)
        conn.rollback()

    finally:
        # Close the cursor and connection
        if conn:
            conn.close()

