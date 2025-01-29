from datetime import datetime
import os
import base64
import threading
from datetime import date
from google.cloud import pubsub_v1
import json
import csv
# Assertion: Every GPS reading must have latitude and longitude coordinates.
def validate_breadcrumb_1(breadcrumb):
    """Validates whether a breadcrumb contains latitude and longitude coordinates.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if the breadcrumb contains latitude and longitude coordinates, False otherwise.
    """
    if "GPS_LATITUDE" not in breadcrumb or "GPS_LONGITUDE" not in breadcrumb:
        return False
    return True

# Every GPS reading must have a timestamp indicating when the reading was taken.
def validate_breadcrumb_2(breadcrumb):
    if "OPD_DATE" not in breadcrumb:
        return False
    return True

# HDOP values for all GPS readings must be within an acceptable range.
def validate_hdop(breadcrumb, acceptable_hdop_range=(0, 10)):
    hdop = breadcrumb.get('GPS_HDOP', None)
    if hdop is not None:
        if hdop < acceptable_hdop_range[0] or hdop > acceptable_hdop_range[1]:
            error_message = f"HDOP value {hdop} for breadcrumb {breadcrumb['EVENT_NO_TRIP']} is outside the acceptable range."
            print(error_message)
            return False
    else:
        print(f"No HDOP value found for breadcrumb {breadcrumb['EVENT_NO_TRIP']}. Skipping validation.")
        return False
    return True

# Assertion: If a reading has latitude, it must have longitude.
def validate_coordinates(breadcrumb):
    """Validates whether a breadcrumb with latitude also contains longitude.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if the breadcrumb contains latitude and longitude coordinates, False otherwise.
    """
    if "GPS_LATITUDE" in breadcrumb and "GPS_LONGITUDE" not in breadcrumb:
        return False
    return True

# Assertion: GPS_LATITUDE must be within a valid range (e.g., -90 to 90 degrees).
def validate_latitude_range(breadcrumb):
    """Validates whether the GPS latitude is within a valid range.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if the GPS latitude is within the valid range, False otherwise.
    """
    latitude = breadcrumb.get('GPS_LATITUDE')
    if latitude is not None:
        if latitude < -90 or latitude > 90:
            return False
    return True

# Assertion: If ACT_TIME is present, OPD_DATE should also be present.
def validate_opd_date_presence(breadcrumb):
    """Validates whether OPD_DATE is present when ACT_TIME is present.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if OPD_DATE is present when ACT_TIME is present, False otherwise.
    """
    if "ACT_TIME" in breadcrumb and "OPD_DATE" not in breadcrumb:
        return False
    return True
# Assertion: GPS_SATELLITES must be a numerical value representing the number of satellites used.
def validate_gps_satellites(breadcrumb):
    """Validates whether GPS_SATELLITES is a numerical value representing the number of satellites used.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if GPS_SATELLITES is a numerical value, False otherwise.
    """
    if "GPS_SATELLITES" in breadcrumb:
        gps_satellites = breadcrumb["GPS_SATELLITES"]
        if not isinstance(gps_satellites, (int, float)):
            return False
    return True
# Assertion: OPD_DATE must be in the format "DDMMMYYYY:HH:MM:SS".
def validate_opd_date_format(breadcrumb):
    """Validates whether OPD_DATE is in the format "DDMMMYYYY:HH:MM:SS".

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if OPD_DATE is in the correct format, False otherwise.
    """
    if "OPD_DATE" in breadcrumb:
        opd_date = breadcrumb["OPD_DATE"]
        try:
            datetime.strptime(opd_date, "%d%b%Y:%H:%M:%S")
        except ValueError:
            return False
    return True

# Assertion: METERS must be a numerical value.
def validate_meters(breadcrumb):
    """Validates whether METERS is a numerical value.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if METERS is a numerical value, False otherwise.
    """
    if "METERS" in breadcrumb:
        meters = breadcrumb["METERS"]
        if not isinstance(meters, (int, float)):
            return False
    return True

# Assertion: ACT_TIME must be a numerical value.
def validate_act_time(breadcrumb):
    """Validates whether ACT_TIME is a numerical value.

    Args:
        breadcrumb (dict): The breadcrumb data.

    Returns:
        bool: True if ACT_TIME is a numerical value, False otherwise.
    """
    if "ACT_TIME" in breadcrumb:
        act_time = breadcrumb["ACT_TIME"]
        if not isinstance(act_time, (int, float)):
            return False
    return True
import pandas as pd
def validate_speed_(dataframe):
    dataframe['speed'] = dataframe['speed'].fillna('0').astype(float)
    invalid_speed_indices = dataframe[dataframe['speed'] < 0].index
    dataframe.loc[invalid_speed_indices, 'speed'] = 0
    # Check if 'SPEED' column has no value and replace with '0'
    #dataframe['SPEED'].fillna('0', inplace=True)

    # Convert 'SPEED' column to numeric
    #dataframe['SPEED'] = pd.to_numeric(dataframe['SPEED'], errors='coerce')

    # Validate 'SPEED' column values (e.g., check for negative values)
    #invalid_speed_indices = dataframe[dataframe['SPEED'] < 0].index

    # Replace invalid values with '0'
    #dataframe.loc[invalid_speed_indices, 'SPEED'] = 0

    
    return dataframe

def validate_speed(csv_file):
    with open(csv_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames
        rows = []
        for row in reader:
            if not row['SPEED']:  # Check if 'SPEED' column has no value
                row['SPEED'] = '0'  # Add '0' if no value found
            rows.append(row)

    # Write back to the CSV file
    with open(csv_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

#validate_speed('sorted_data.csv')
