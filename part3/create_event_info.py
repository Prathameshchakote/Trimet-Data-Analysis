import psycopg2
from io import StringIO
import pandas as pd
def map_direction(direction):
    if direction == 'S':
        return 'Saturday'
    elif direction == 'U':
        return 'Sunday'
    else:
        return 'Weekday'
# Function to copy data from DataFrame to PostgreSQL table
def copy_from_dataframe(conn, df, table_name):
    # Create a StringIO object to temporarily hold the DataFrame data
    buffer = StringIO()
    print(df.columns)
    df = df[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]
    print(df.columns)
    direction_mapping = {
    0: 'Out',
    1: 'Back'
}
    
 
    #df['direction'] = df['direction'].fillna(0)
    df.loc[:, 'direction'] = df['direction'].apply(map_direction)
    valid_service_keys = ['Weekday', 'Saturday', 'Sunday']
    df.loc[:, 'service_key'] = df['service_key'].apply(lambda x: x if x in valid_service_keys else 'Weekday')
    df['direction'] = pd.to_numeric(df['direction'], errors='coerce').fillna(0)

    #df['direction'] = df['direction'].map(direction_mapping)
    df['direction'] = df['direction'].astype(int).map(direction_mapping)
    #df.drop_duplicates(subset='trip_id', keep='last', inplace=True)
    # Write the DataFrame data to the buffer in CSV format
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    # Create a cursor object
    cursor = conn.cursor()
    try:
        # Execute the COPY command to copy data from the buffer to the PostgreSQL table
        cursor.copy_from(buffer, table_name, sep=',')
        # Commit the transaction
        conn.commit()
        print("Data copied successfully to table:", table_name)
    except (Exception, psycopg2.DatabaseError) as error:
        # Rollback the transaction in case of an error
        print("Error: %s" % error)
        conn.rollback()
    finally:
        # Close the cursor
        cursor.close()
