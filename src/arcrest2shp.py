import concurrent.futures
from arcrest2shp_utils import create_folder, create_csv, process_links, bbox_shp, download_data, clip_geojson_export_shp

def arcrest2shp (url_base, shp, out_path, num_threads = 10):
    def process_url(url):
        # Download data from the URL and save it as GeoJSON
        geojson_out_path = download_data(url, shp, export_path)
        # Clip the downloaded GeoJSON with the shapefile and save the clipped result as a shapefile
        clip_geojson_export_shp(shp, geojson_out_path, shp_out_path)
    """
	Download and process data from multiple URLs.
	Parameters:
	- url_base (str): The base URL for retrieving data.
	- shp (str): The path to the shapefile.
	- out_path (str): The path to the output folder.
	- crs (int, optional): The coordinate reference system code. Defaults to 7844.
	- num_threads (int, optional): The number of threads to use for concurrent processing. Defaults to 10.
	Returns:
	None
	"""
    # Create necessary folders for the data processing
    export_path = create_folder (out_path) # Main folder
    geojson_out_path = create_folder (export_path, 'geojson') # GeoJSON folder
    shp_out_path = create_folder (export_path, 'shp') # Shapefile folder 
    # Create a CSV file in the export_path
    create_csv (export_path)

    # Retrieve all links from the URL base
    cache = {}  # Dictionary to cache retrieved links
    visited = set()  # Set to track visited URLs
    process_links(url_base, cache, visited)
    #process_links_mthread (url, cache, visited, num_threads) #Not used yet
    
    # Filter out links containing 'FS/MapServer' in the URL
        # Edit in the future for other rest servers -- thin is for dataWa
    filtered_data = [value for value in list(visited) if 'FS/MapServer' not in value]

    # Use ThreadPoolExecutor to execute the function concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(process_url, filtered_data)