import pandas as pd
import psycopg2

def copy_trip_data(conn, trip_data):
    try:
        cur = conn.cursor()

        # Drop duplicate entries based on trip_id and vehicle_id
        #trip_data = trip_data.drop_duplicates(subset=['vehicle_id'])
        trip_data = trip_data.drop_duplicates(subset=['trip_id'])
        # Create a buffer to hold the data
        from io import StringIO
        buffer = StringIO()
        trip_data[['trip_id', 'vehicle_id']].to_csv(buffer, sep=',', header=False, index=False)
        buffer.seek(0)

        # Use COPY command to insert data into Trip table
        cur.copy_from(buffer, 'trip', sep=',', columns=('trip_id', 'vehicle_id'))

        # Commit the transaction
        conn.commit()

        print("Trip data copied successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error copying trip data: {error}")
        conn.rollback()  # Rollback the transaction in case of an error
    finally:
        # Close the cursor
        cur.close()

