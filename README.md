# 🚍 TriMet Data Engineering Pipeline

## 📌 Overview

This project builds an end-to-end data engineering pipeline for the **TriMet** public transit dataset. It performs:

- Daily ingestion of GPS sensor data.
- Parsing of breadcrumb-level readings.
- Asynchronous data transport via **Google Cloud Pub/Sub**.
- Storage in **Google Cloud Storage**.
- Validation and transformation for analytical use.
- Enhancement with absolute timestamps.
- Loading into a **PostgreSQL** database.
- Integration with route stop events.
- Visualization for insights.

---

## 🗂 Dataset Description

### 📍 About TriMet

TriMet serves the Portland, Oregon metro area with 600+ buses across 80+ routes. Wireless GPS sensors on buses report locations every 5 seconds, generating nearly **2 million** readings per day.

**Included Datasets:**

- **Breadcrumb Data** – 5-second GPS logs from buses.
- **Stop Event Data** – Logs of each bus stopping at a route stop.

---

## 🏗 System Design

### 🔧 Architecture Components

1. **Google Cloud Pub/Sub**
   - **Publisher**: Fetches breadcrumb data from API and publishes to topic.
   - **Subscriber**: Validates messages, stores in Cloud Storage, and inserts into PostgreSQL.

2. **Google Cloud Storage**
   - Stores validated data as compressed CSV files.

3. **PostgreSQL**
   - Stores structured, cleaned, and enriched trip data.

4. **Data Validation**
   - Removes invalid or out-of-bound GPS data and inconsistent timestamps.

5. **Data Transformation & Enhancement**
   - Adds route and trip metadata for integration with stop event data.

---

## 🗃 Database Schema

- `Breadcrumb`
- `Trip`
- `Event_Info`

The `Trip` table is enriched by joining it with `Event_Info` using SQL.

---

## ⚙️ Pipeline Workflow

1. **Data Ingestion**
   - Fetch daily GPS breadcrumb data from server.
   - Publish messages to **Pub/Sub**.

2. **Validation**
   - Apply filters for speed, location bounds, and timestamps.
   - Discard erroneous records.

3. **Storage & Processing**
   - Write cleaned messages to **GCP Storage**.
   - Convert to CSV → Compress → Upload.
   - Load into PostgreSQL for analysis.

4. **Data Integration**
   - Join `Breadcrumb`, `Trip`, and `Event_Info` to construct enriched datasets.

---

## 📊 Data Statistics

### Breadcrumb Data

| Metric               | Value             |
|----------------------|-------------------|
| Total Size           | 954.72 MB         |
| Total Records        | 14,079,025        |
| Weekday Avg. Records | 320,529 (\~21.7 MB) |
| Weekend Avg. Records | 196,289 (\~17.4 MB) |

### Stop Event Data

| Metric               | Value              |
|----------------------|--------------------|
| Total Size           | 37.30 MB           |
| Total Records        | 550,107            |
| Weekday Avg. Records | 39,569 (\~2.68 MB) |
| Weekend Avg. Records | 23,323 (\~1.58 MB) |

---

## 📈 Visualizations

1. **Goose Hollow GPS Points**
   - Shows GPS locations near Goose Hollow on a Monday from 9 AM – 5 PM.

2. **Route Visualization**
   - Plots the full route for selected trips with start/end/intermediate stops.

---

## 🚧 Challenges Faced

- VM crashes under data volume pressure.
- Handling large dataset integration across tables.
- Crafting a robust validation strategy.
- Pre-multithreading performance bottlenecks.
- SQL primary key constraint violations during inserts.

---

## ⭐ Additional Features

- Pipeline deployed across multiple virtual machines.
- Archived data in **GCP Cloud Storage** for redundancy and backup.

---

## 📚 Conclusion

This project delivers practical, end-to-end exposure to:

- Real-world sensor data ingestion.
- Cloud-native pipeline design using **Pub/Sub**, **Storage**, and **PostgreSQL**.
- Data validation and transformation techniques.
- Integration and visualization for actionable insights.

---

