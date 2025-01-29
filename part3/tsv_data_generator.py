import pandas as pd
import numpy as np
import psycopg2

def filter_and_copy_to_postgres():
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

        # Execute SQL query
        cursor.execute("""
        SELECT DISTINCT b.longitude, b.latitude, b.speed FROM Trip t
JOIN BreadCrumb b ON t.trip_id = b.trip_id
WHERE (b.latitude BETWEEN 45.50921922110612 AND 45.513848810065525)
AND (b.longitude BETWEEN -122.68811436369573 AND -122.68063228685133)
And date(b.tstamp) = date '2023-01-20'
AND b.tstamp >= '2023-01-20 09:14:01'
AND b.tstamp <= '2023-01-20 11:00:00';
;


        """)

        # Fetch results into DataFrame
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=columns)

        # Save DataFrame to file
        #df.to_csv("output.csv", index=False)
        df.to_csv("output.tsv", sep='\t', index=False)

        print("Data saved to output.csv.")

    except (Exception, psycopg2.Error) as error:
        print("Error while executing query or saving data:", error)

    finally:
        # Close the cursor and connection
        if conn:
            conn.close()

# Assuming input_dataframe is defined somewhere before calling this function
# e.g., input_dataframe = pd.read_csv("input_data.csv")
filter_and_copy_to_postgres()

