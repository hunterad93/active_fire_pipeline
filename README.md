# active_fire_pipeline

Code for a google cloud function that processes VIIRS data and uploads polygons encompassing active fires. Several NASA satellite products are requested from their API as CSV files: https://firms.modaps.eosdis.nasa.gov/api/area/. The requested data is filtered to the last 24 hours, and the observations are spatially clustered. Clusters are then filtered to only include instances where high confidence observations are found across all satellite products and the total count of observations passes a certain threshold. Finally the clusters that make it past these filters are turned into convex hull polygons and uploaded as GeoJSON to a GBQ database.

# viirs_upload_cloud_func
This folder contains `main.py` which is the cloud function script, with `requirements.txt` specifying the necessary packages.

`main.py` is called via a request with its arguments specified in JSON like this.
{
  "api_key": "",
  "bbox": "-171,16,-66,74",
  "products": ["VIIRS_SNPP_NRT", "VIIRS_NOAA21_NRT", "VIIRS_NOAA20_NRT"],
  "min_cluster_size": 25,
  "required_high_confidence": 1
}