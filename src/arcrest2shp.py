import concurrent.futures
from arcrest2shp_utils import create_folder, create_csv, process_links, download_data, check_geojson

def arcrest2shp (url_base, shp, out_path, num_threads = 10):
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
    csv_path = create_csv (export_path)

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
        executor.map(download_data, filtered_data)

    check_geojson (geojson_out_path, csv_path)