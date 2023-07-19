import concurrent.futures
from dwae_utils import create_folder, create_csv, process_links, bbox_shp, download_data, clip_geojson_export_shp


def dwae (url_base, shp, out_path, crs=7844, num_threads = 10):
    def process_url(url):
        geojson_out_path = download_data(url, shp, export_path)
        clip_geojson_export_shp(shp, crs, geojson_out_path, shp_out_path)

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

    ##main folder
    export_path = create_folder (out_path)
    ##geojson
    geojson_out_path = create_folder (export_path, 'geojson')
    ##shp
    shp_out_path = create_folder (export_path, 'shp')

    create_csv (export_path)

    #retrieve all links
    cache = {}  # Dictionary to cache retrieved links
    visited = set()  # Set to track visited URLs

    process_links(url_base, cache, visited)

    # process_links (url_base, cache, visited, num_threads)

    filtered_data = [value for value in list(visited) if 'FS/MapServer' not in value]

    #retrieve bbox
    bbox, crs = bbox_shp (shp, crs)

    # Use ThreadPoolExecutor to execute the function concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(process_url, filtered_data)


