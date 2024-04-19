from datetime import datetime, timedelta
from io import StringIO
import geopandas as gpd
import requests
import pandas as pd
from sklearn.cluster import DBSCAN
from shapely.geometry import MultiPoint
import json
from google.cloud import bigquery

def get_firms_data(api_key, bbox, product, days_of_data = 2, date=None):
    '''
    Connect with FIRMS API to access data from a specified date, bbox, product, and range of days
    and return it as a GeoDataFrame. If no date is specified, defaults to today.
    
    :param api_key: str, from NASA email, provided in cron job's request headers
    :param bbox: str, bbox of the region of interest in the format "minLongitude,minLatitude,maxLongitude,maxLatitude", provided in cron job's request headers
    :param date: str, date in '%Y-%m-%d' format. If not provided, defaults to today.
    :return: GeoDataFrame of fire detection data with columns corresponding to the FIRMS API response
    '''
    
    base_url = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv/'

    # Request `days_of_data` worth of data, before filtering via the acq_date/time
    url = f'{base_url}{api_key}/{product}/{bbox}/{days_of_data}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception if the request was unsuccessful
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data: {e}")
    else:
        data = StringIO(response.text)  # Convert text response to file-like object
        df = pd.read_csv(data)  # Read data into a DataFrame


    # Convert the DataFrame to a GeoDataFrame, setting the geometry from the latitude and longitude columns
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))

    # Drop unnecessary columns
    columns_to_keep = ['latitude', 'longitude', 'confidence', 'geometry', 'acq_date', 'acq_time']
    gdf = gdf[columns_to_keep]

    # Add a column indicating the product
    gdf['product'] = product

    return gdf

def filter_by_datetime(gdf, days=1):
    """
    Filter the GeoDataFrame to include only rows from the last 24 hours.
    
    :param gdf: GeoDataFrame with 'acq_date' and 'acq_time' columns
    :return: GeoDataFrame with rows from the last 24 hours
    """
    # Convert 'acq_time' to a string and pad it with zeros to ensure it has four digits
    gdf['acq_time'] = gdf['acq_time'].astype(str).str.zfill(4)

    # Extract the hours and minutes from 'acq_time'
    gdf['hour'] = gdf['acq_time'].str[:2]
    gdf['minute'] = gdf['acq_time'].str[2:]

    # Combine 'acq_date', 'hour', and 'minute' into a single datetime column
    gdf['datetime'] = pd.to_datetime(gdf['acq_date'] + ' ' + gdf['hour'] + ':' + gdf['minute'])

    # Sort the GeoDataFrame by 'datetime'
    gdf = gdf.sort_values('datetime')
    print(len(gdf))
    # Get the latest time in the GeoDataFrame
    latest_time = gdf['datetime'].max()
    print(latest_time)
    # Get the time 24 hours before the latest time
    one_day_before_latest = latest_time - pd.Timedelta(days=days)

    # Filter rows from the last 24 hours based on the latest time
    gdf = gdf[gdf['datetime'] >= one_day_before_latest]
    print(len(gdf))
    return gdf


def cluster_fires(gdf, eps=0.01, min_samples=1):
    """
    Given a GeoDataFrame of fire points, create spatial clusters
    :param gdf: GeoDataFrame of fire points
    :param eps: The maximum distance between two samples for one to be considered as in the neighborhood of the other
    :param min_samples: The number of samples in a neighborhood for a point to be considered as a core point
    :return: GeoDataFrame of fire points with an additional column 'label' indicating the cluster each point belongs to
    """

    # Perform DBSCAN clustering
    coords = gdf[['longitude', 'latitude']].values
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(coords)

    # Add cluster labels to the dataframe
    gdf['label'] = db.labels_

    return gdf

def filter_clusters_with_product_confidence(gdf, min_cluster_size, required_high_confidence_per_product):
    """
    Filter out clusters that have fewer points than the threshold, and ensure at least one high confidence point
    from each product exists within the cluster.
    
    :param gdf: GeoDataFrame of fire points with 'label' column indicating the cluster each point belongs to
    :param min_cluster_size: Minimum number of points in a cluster for it to be kept
    :param required_high_confidence_per_product: Minimum number of high confidence points from each product in a cluster for it to be kept
    :return: GeoDataFrame of fire points in clusters that meet both thresholds
    """

    # Count the number of points in each cluster
    cluster_counts = gdf['label'].value_counts()

    # Filter out clusters smaller than the minimum size
    valid_clusters_by_size = cluster_counts[cluster_counts >= min_cluster_size].index

    # Filter for high confidence points
    high_confidence_gdf = gdf[gdf['confidence'] == 'h']

    # Ensure at least one high confidence point from each product within the cluster
    valid_clusters_by_product_confidence = high_confidence_gdf.groupby('label')['product'].nunique()
    valid_clusters_by_product_confidence = valid_clusters_by_product_confidence[valid_clusters_by_product_confidence >= required_high_confidence_per_product].index

    # Find the intersection of clusters that meet both criteria
    valid_clusters = set(valid_clusters_by_size) & set(valid_clusters_by_product_confidence)

    # Filter the GeoDataFrame to include only valid clusters
    gdf = gdf[gdf['label'].isin(valid_clusters)]

    return gdf

def create_cluster_polygons(gdf):
    """
    Given a GeoDataFrame of clustered fire points, create a polygon for each cluster
    :param gdf: GeoDataFrame of fire points with 'label' column indicating the cluster each point belongs to
    :return: List of dictionaries with acquisition datetime, WKT, and GeoJSON strings for each cluster
    """
    print('creating polygon')
    # Group the GeoDataFrame by the cluster labels
    grouped = gdf.groupby('label')

    cluster_info = []

    for label, group in grouped:
        if label == -1:  # Skip noise points
            continue
        # Create a MultiPoint object from the fire points, then create a polygon from the convex hull of the points
        polygon = MultiPoint(group.geometry.tolist()).convex_hull
        # Convert the most frequently occurring acquisition date to datetime
        acq_datetime = pd.to_datetime(group['datetime'].mode()[0])
        # Prepare the dictionary
        cluster_info.append({
            'acq_datetime': acq_datetime,
            'fire_wkt': polygon.wkt,
            'fire_geojson': json.loads(gpd.GeoSeries([polygon]).to_json())['features'][0]['geometry']
        })

    return cluster_info

def upload_to_bigquery(cluster_info):
    """
    Uploads each polygon data as a separate row to BigQuery.

    :param cluster_info: List of dictionaries with acquisition datetime, WKT, and GeoJSON strings for each cluster.
    """
    # Initialize a BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    dataset_id = 'geojson_predictions'
    table_id = 'geoms'

    # Get the table
    table = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table)

    rows_to_insert = []

    for cluster in cluster_info:
        row = {
            'acq_datetime': cluster['acq_datetime'].strftime('%Y-%m-%dT%H:%M:%SZ'),
            'datetime_added': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'fire_wkt': cluster['fire_wkt'],
            'fire_geojson': json.dumps(cluster['fire_geojson']),
        }
        rows_to_insert.append(row)

    # Insert the rows
    errors = client.insert_rows_json(table, rows_to_insert)

    # Check if any errors occurred
    if errors:
        print('Errors:', errors)
    else:
        print(f'{len(rows_to_insert)} rows inserted successfully.')

def FIRMS_GEOJSON_UPDATE(request):
    # Check if request is a dictionary for local testing or a Flask request object
    if isinstance(request, dict):
        request_json = request
    else:
        request_json = request.get_json(silent=True)
        
    api_key = request_json.get('api_key')
    bbox = request_json.get('bbox', 'world')
    products = request_json.get('products', ["VIIRS_SNPP_NRT", "VIIRS_NOAA21_NRT", "VIIRS_NOAA20_NRT"])
    min_cluster_size = request_json.get('min_cluster_size', 25)  # Default value set to 40 if not specified
    required_high_confidence = request_json.get('required_high_confidence', 1)  # Default value set to 3 if not specified

    # Retrieve data using the provided API key, bounding box, and list of products
    gdfs = [get_firms_data(api_key=api_key, bbox=bbox, product=product) for product in products]
    gdfs = [filter_by_datetime(gdf) for gdf in gdfs]

    combined_gdf = pd.concat(gdfs, ignore_index=True)
    # Cluster the combined data points
    clustered_combined_gdf = cluster_fires(combined_gdf)
    # Filter out small clusters and clusters with too few points or no high confidence point
    filtered_combined_clusters = filter_clusters_with_product_confidence(clustered_combined_gdf, min_cluster_size=min_cluster_size, required_high_confidence_per_product=required_high_confidence)
    # Create a polygon for each cluster
    cluster_info = create_cluster_polygons(filtered_combined_clusters)
    upload_to_bigquery(cluster_info)

    return 'Successfully processed and uploaded data', 200



### COMMENT THIS FINAL SECTION IN TO TEST LOCALLY ###

# #function calling for local testing
# import os

# # Call the FIRMS_GEOJSON_UPDATE function with the API key from the environment variable
# FIRMS_GEOJSON_UPDATE({
#     'api_key': os.environ.get('FIRMS_API_KEY'),
#     'bbox': '-171,16,-66,74',  
#     'products': ["VIIRS_SNPP_NRT", "VIIRS_NOAA21_NRT", "VIIRS_NOAA20_NRT"],  
#     'min_cluster_size': 25,  
#     'required_high_confidence': 1, 
# })
