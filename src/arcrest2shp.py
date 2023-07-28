import concurrent.futures
from arcrest2shp_utils import create_folder, create_csv, process_links, download_data, check_geojson, create_csv_error_log

def arcrest2shp (url_base, shp, out_path, num_threads = 10):
    """
    Download and process data from multiple URLs.

    Parameters:
        url_base (str): The base URL for retrieving data.
        shp (str): The path to the shapefile.
        out_path (str): The path to the output folder.
        num_threads (int, optional): The number of threads to use for concurrent processing. Defaults to 10.

    Returns:
        None

    Note:
        - The function downloads and processes data from multiple URLs based on the `url_base`.
        - It creates necessary folders for data processing, including a main folder, a GeoJSON folder,
          and a Shapefile folder inside the `out_path`.
        - The function creates CSV files to summarize vectors and rasters, and an error log.
        - It retrieves all links from the `url_base` and filters out links containing 'FS/MapServer' in the URL.
        - The filtered data links are processed concurrently using ThreadPoolExecutor and `num_threads` threads.
        - GeoJSON files are checked and updated, and vector information is summarized in the CSV file.

    Parameters Description:
        - url_base (str): The base URL from where data will be retrieved.
        - shp (str): The path to the shapefile used for clipping GeoJSON data.
        - out_path (str): The path to the output folder where processed data and CSV files will be stored.
        - num_threads (int, optional): The number of threads to use for concurrent processing. Defaults to 10.

    Example:
        arcrest2shp('https://example.com/data/', 'path/to/shapefile.shp', 'output_folder/', num_threads=4)
    """
    # Create necessary folders for the data processing
    export_path = create_folder (out_path) # Main folder
    geojson_out_path = create_folder (export_path, 'geojson') # GeoJSON folder
    shp_out_path = create_folder (export_path, 'shp') # Shapefile folder 
    
    # Create a CSV file in the export_path to summarise vectors
    csv_path_vector = create_csv (export_path, 'vector')
    # Create a CSV file in the export_path to summarise rasters
    create_csv (export_path, 'raster')
    # Create a CSV file in the export_path to summarise errors
    create_csv_error_log (export_path)
    
    # Retrieve all links from the URL base
    cache = {}  # Dictionary to cache retrieved links
    visited = set()  # Set to track visited URLs
    process_links(url_base, cache, visited)
    #process_links_mthread (url, cache, visited, num_threads) #Not used yet
    
    # Filter out links containing 'FS/MapServer' in the URL
        # Edit in the future for other rest servers -- thin is for dataWa
    filtered_data = [value for value in list(visited) if 'FS/MapServer' not in value]
    #arg list for multithread input
    args_list = list(zip(filtered_data, [shp] * len(filtered_data), 
                [export_path] * len(filtered_data), [shp_out_path] * len(filtered_data)))
    # Use ThreadPoolExecutor to execute the function concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(download_data, args_list)
    # Check and update geojson files
    check_geojson (geojson_out_path, csv_path_vector)