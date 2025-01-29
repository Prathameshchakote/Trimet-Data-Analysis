import csv
from geojson import Feature, FeatureCollection, Point

features = []

with open('output (3).tsv', newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t')  # Use '\t' as the delimiter
    for row in reader:
        # Extract latitude, longitude, and speed arrays from each row
        trip_id = row['trip_id']
        route_id = row['route_id']
        service_key = row['service_key']
        direction = row['direction']
        trip_duration = row['trip_duration']
        latitude_points_str = row['latitude_points']
        longitude_points_str = row['longitude_points']
        speed_points_str = row['speed_points']

        # Parse latitude, longitude, and speed strings to arrays of floats
        latitude_points = [float(point) for point in latitude_points_str.strip('[]').split(',')]
        longitude_points = [float(point) for point in longitude_points_str.strip('[]').split(',')]
        speed_points = [float(point) for point in speed_points_str.strip('[]').split(',')]

        # Create features for each coordinate and speed
        for latitude, longitude, speed in zip(latitude_points, longitude_points, speed_points):
            features.append(
                Feature(
                    geometry=Point((longitude, latitude)),
                    properties={'speed': speed}  # Assign speed as a property
                )
            )

# Create a FeatureCollection
collection = FeatureCollection(features)

# Write the FeatureCollection to a GeoJSON file
with open("visualisation_5A.geojson", "w") as f:
    f.write('%s' % collection)
