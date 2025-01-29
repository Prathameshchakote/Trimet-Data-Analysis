import csv
from geojson import Feature, FeatureCollection, Point

features = []

with open('output (7).tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')  # Use '\t' as the delimiter
    next(reader)  # Skip the header row
    for row in reader:
        # Extract longitude, latitude, and speed from each row
        x, y, speed = row
        try:
            latitude, longitude = float(y), float(x)
            features.append(
                Feature(
                    geometry=Point((longitude, latitude)),
                    properties={'speed': float(speed)}  # Convert speed to float
                )
            )
        except ValueError:
            continue

# Create a FeatureCollection
collection = FeatureCollection(features)

# Write the FeatureCollection to a GeoJSON file
with open("visualisation_3.geojson", "w") as f:
    f.write('%s' % collection)
