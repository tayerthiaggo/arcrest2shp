import os
import geopandas as gpd
from shapely.geometry import Polygon
import subprocess
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import csv
import datetime
import concurrent.futures
import time
import warnings

def create_folder (out_path, folder_name='extracted_data'):
    """
    Create a folder in the specified output path.

    Args:
        out_path (str): Output path where the folder will be created.
        folder_name (str): Name of the folder to be created.

    Returns:
        str: Export path of the created folder.

    Raises:
        OSError: If there is an error creating the folder.
    """
    export_path = os.path.join(out_path, folder_name)
    try:
        # Create the folder
        os.makedirs(export_path, exist_ok=True)
        print(f"Folder created: {export_path}")
    except FileExistsError:
        print('Folder already exists', export_path)
    return export_path
def retrieve_links (url, cache):
    """
    Retrieve all links from a webpage.

    Args:
        url (str): The URL of the webpage.
        cache (dict): A dictionary to cache previously retrieved links.

    Returns:
        list: A list of links found on the webpage.
    """
    if url in cache:
            return cache[url]

    max_retries = 20
    retry_count = 0
    # Send a GET request to the URL
    while retry_count <= max_retries:
        try:
            response = requests.get(url)
            # Check if the request was successful (HTTP status code 200)
            if response.status_code != 200:
                print(url, "\nRequest failed with status code:", response.status_code)
                return []
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            # Find all <li> elements
            li_tags = soup.find_all('li')
            links = []
            for li in li_tags:
                if li.find_all('a'):
                    # Find all <a> elements within the <li>
                    a_tags = li.find_all('a')
                    for a in a_tags:
                        # Extract the link URL from the <a> element
                        link = urljoin(url, a.get('href'))
                        links.append(link)
            return links
        
        except requests.exceptions.RequestException as e:
            # print(f"An error occurred: {str(e)}")
            # print("Retrying in 5 seconds...")
            time.sleep(5)  # Wait for 5 seconds before retrying
            retry_count += 1
    print('Max_retries achieved for the following url: ', url)
    return None
def process_links(url, cache, visited):
    """
    Recursively process links from a starting URL.

    Args:
        url (str): The starting URL to process.
        cache (dict): A dictionary to cache previously retrieved links.
        visited (set): A set to keep track of visited URLs.

    Returns:
        list: A list of processed links.
    """
    if url in visited:
        return []
    visited.add(url)  # Mark the current URL as visited
    links = retrieve_links(url, cache)
    processed_links = []
  
    for link in links:
        if link not in visited:
            # Process the link recursively
            processed_links.append(link)
            processed_links.extend(process_links(link, cache, visited))
    return processed_links
## Not used (API protection against attack)
def process_links_mthread (url, cache, visited, num_threads):
    """
    Recursively process links from a starting URL.

    Args:
        url (str): The starting URL to process.
        cache (dict): A dictionary to cache previously retrieved links.
        visited (set): A set to keep track of visited URLs.

    Returns:
        list: A list of processed links.
    """
    if url in visited:
        return []
    visited.add(url)  # Mark the current URL as visited
    links = retrieve_links(url, cache)
    processed_links = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for link in links:
            if link not in visited:
                # Process the link asynchronously
                future = executor.submit(process_links, link, cache, visited, num_threads)
                futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            processed_links.extend(future.result())

    return processed_links
def shp_info (shp):
    # Read the shapefile using geopandas
    shp_gdf = gpd.read_file(shp)
    # # Read the shapefile Coordinate reference system (CRS) code
    shp_crs = shp_gdf.crs
    return shp_gdf, shp_crs 
def run_esri2geojson (url, shp_bbox, crs, layer_name, export_path):
    """
    Converts data from an Esri service to GeoJSON format using the 'esri2geojson' command-line tool.

    Args:
        url (str): The URL of the Esri service.
        shp_bbox (list): A list of four float values representing the bounding box coordinates in the order [minx, miny, maxx, maxy].
        crs (str): The coordinate reference system (CRS) identifier.
        layer_name (str): The name of the layer to be extracted.
        export_path (str): The directory path where the exported GeoJSON file will be saved.

    Returns:
        str: The file path of the generated GeoJSON file.

    Raises:
        subprocess.CalledProcessError: If the 'esri2geojson' command fails to execute.
    """
    geojson_out_path = os.path.join(export_path, 'geojson', f'{layer_name}.geojson')
    # Check if a file with the same name exists, generate a unique name with incremental count if needed
    count = 0
    while os.path.exists(geojson_out_path):
        count += 1
        geojson_out_path = os.path.join(
            export_path, 'geojson', f'{layer_name}_{str(count)}.geojson'
        )
    # Edit geometry for input
    geometry_str = ''.join(['geometry=', ','.join([str(num) for num in shp_bbox])])
    # Edit crs for input 
    crs_str = ''.join(['inSR=', str(crs)]) 
    # Concatenate the variables with spaces
    command = ' '.join(['esri2geojson',url, '-p', geometry_str, '-p', crs_str, geojson_out_path])
    # Execute the command
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # Check the result
    if result.returncode == 0:
        pass
    else:
        # Log the error in 'error_log.csv' in the export_path
        data = [layer_name, url, datetime.date.today()]
        write_csv (os.path.join(export_path, 'error_log.csv'), data)
        print(f'{url} An error occurred while executing the run_esri2geojson.')
    return geojson_out_path
def clip_geojson_export_shp (shp_crs, shp_gdf, geojson_out_path, shp_out_path):
    """
    Clips a GeoJSON file using a polygon GeoDataFrame and exports the clipped data to a shapefile.

    Args:
        shp_crs (str): The Coordinate Reference System (CRS) of the shapefile.
        shp_gdf (geopandas.GeoDataFrame): The polygon GeoDataFrame representing the area of interest for clipping.
        geojson_out_path (str): The file path of the input GeoJSON file to be clipped.
        shp_out_path (str): The directory path where the exported shapefile will be saved.

    Returns:
        tuple: A tuple containing two elements:
            - bool: True if the clipped GeoDataFrame is empty, False otherwise.
            - str: The file path of the exported shapefile.
    """
    # Suppress the UserWarning
    warnings.filterwarnings("ignore", message="Column names longer than 10 characters will be truncated when saved to ESRI Shapefile.")
    # Read the GeoJSON and shapefile into GeoDataFrames   
    geojson_gdf = gpd.read_file(geojson_out_path)

    # Check if the GeoJSON GeoDataFrame is not empty
    if not geojson_gdf.empty:       
        # Convert the GeoJSON GeoDataFrame to the shapefile coordinate reference system (CRS)
        geojson_gdf = geojson_gdf.to_crs(shp_crs)
        # Clip the GeoJSON with the shapefile
        clipped_gdf = gpd.clip(geojson_gdf, shp_gdf)

        # Check if the clipped GeoDataFrame is not empty
        if not clipped_gdf.empty:
            # Extract the file name without extension
            output_shapefile = f'{os.path.splitext(os.path.basename(geojson_out_path))[0]}.shp'
            # Create the full output shapefile path
            out_path_shp = os.path.join(shp_out_path, output_shapefile)
            # Export the clipped GeoDataFrame to a shapefile
            clipped_gdf.to_file(out_path_shp)
    # Return True if the clipped GeoDataFrame is empty, otherwise False
    return clipped_gdf.empty, out_path_shp
def check_layer_type (soup):
    """
    Checks the type of layer in the HTML content and returns 'Vector' or 'Raster'.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML content.

    Returns:
        str: The layer type as 'Vector' if the content indicates a vector layer,
        'Raster' if the content indicates a raster layer, or None if the layer type is not recognized.
    """
    # Find all tags that contain the text "esriGeometry"
    cells = soup.find_all(lambda tag: tag.name == 'b' and 'Geometry Type:' in tag.text)
    # Check if any cell contains the desired text
    contains_esriGeometry = any('esriGeometry' in cell.next_sibling.strip() for cell in cells)
    # Find all tags that contain the text "esriGeometry"
    cells = soup.find_all(lambda tag: tag.name == 'b' and 'Type:' in tag.text)
    # Check if any cell contains the desired text
    contains_raster = any('Raster' in cell.next_sibling.strip() for cell in cells)

    if contains_esriGeometry:
        return 'Vector'
    elif contains_raster:
        return 'Raster'
    else:
        pass
def info_to_sheets (export_path, soup, layer_name, url, out_path_shp, data_type):
    """
    Write extracted information to a CSV file.

   Args:
        export_path (str): The directory path where the CSV file will be saved.
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML content.
        layer_name (str): The name of the layer.
        url (str): The URL of the layer.
        out_path_shp (str): The file path of the exported shapefile (only applicable for 'vector' data_type).
        data_type (str): The type of data being extracted ('vector' or 'raster').

    Returns:
        None
    """
    # Define the file path
    file_path = os.path.join(export_path, f'extracted_data_{data_type}.csv')
    # Extract the geometry type from the soup object
    geometry_type = soup.find('b', string='Geometry Type:').next_sibling.strip()
    # Extract the description text from the soup object
    description_text = soup.find('b', string='Description: ').next_sibling.strip()
    # Extract the source from the layer_name by splitting it at underscores and taking the first part
    source = layer_name.split('_')[0]
    # Get the current date
    extraction_date = datetime.date.today()
    # Create a list containing the extracted data
    data = [source, layer_name, geometry_type, description_text, url, extraction_date, out_path_shp]
    # Write the extracted data to the CSV file
    write_csv (file_path, data)
def create_csv (export_path, data_type):
    # Define the file path
    csv_path = os.path.join(export_path, f'extracted_data_{data_type}.csv')
    #define columns
    columns = ['Source', 'Name', 'Geometry Type', 'Description', 'URL', 'Extraction Date', 'Out Path']
    # Check if the file exists
    file_exists = os.path.isfile(csv_path)
    # Write the extracted information to the CSV file
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row if the file is newly created
        if not file_exists:
            writer.writerow(columns)
    return csv_path
def create_csv_error_log (export_path):
    # Define the file path
    csv_path = os.path.join(export_path, 'error_log.csv')
    #define columns
    columns = ['Name', 'URL', 'Extraction Date']
    # Check if the file exists
    file_exists = os.path.isfile(csv_path)
    # Write the extracted information to the CSV file
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row if the file is newly created
        if not file_exists:
            writer.writerow(columns)
    return csv_path
def write_csv (file_path, data):
    # Check if the file exists
    file_exists = os.path.isfile(file_path)
    # Write the extracted information to the CSV file
    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write the new row of data
        writer.writerow(data)
def filter_layer_name_and_crs (soup):
    """
    Filter and format the layer name extracted from the HTML soup and extract the coordinate reference system (CRS).

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML content.

    Returns:
        tuple: A tuple containing two elements:
            - str: The filtered and formatted layer name.
            - int: The extracted coordinate reference system (CRS) identifier.
    """
    # Check if the page has a 'Name' section
    name_tag = soup.find('b', string='Name:')
    # Get the text of the next sibling of the 'Name' section
    layer_name = name_tag.next_sibling.strip()
    # Extract content within brackets
    # The pattern '\((.*?)\)' matches anything within parentheses and captures the content inside the parentheses
    values_in_parentheses = re.findall(r'\((.*?)\)', layer_name)
    # Find the values within parentheses that contain both letters and numbers
    valid_values = [value for value in values_in_parentheses if re.match(r'^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9]+', value)]

    # If multiple valid values were found, select the last one
    if len(valid_values) > 1:
        valid_values = valid_values[-1]
    else:
        valid_values = ''.join(valid_values)

    if valid_values:
        # Remove the matched values within parentheses from the layer_name
        layer_name = layer_name.replace(f"({valid_values})", '').strip()
    else:
        # The layer name does not have values within parentheses, check for 'Parent Layer' section
        parent_layer_tag = soup.find('b', string='Parent Layer:')

        if parent_layer_tag:
            # Get the text of the next sibling of the 'Parent Layer' section
            parent_layer = parent_layer_tag.next_sibling.strip()
            # Check if there is a link inside 'Parent Layer'
            parent_layer_link = parent_layer_tag.find_next_sibling('a')

            if parent_layer_link:
                # Extract content within parentheses from the link
                values_in_parentheses = re.findall(r'\((.*?)\)', ''.join(parent_layer_link))
                # Find the values within parentheses that contain both letters and numbers
                valid_values = [value for value in values_in_parentheses if re.match(r'^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9]+', value)]
                # If multiple valid values were found, select the last one
                if len(valid_values) > 1:
                    valid_values = valid_values[-1]
                else:
                    valid_values = ''.join(valid_values)

    # Concatenate the values within parentheses (if any) and the layer_name
    layer_name = f"{valid_values}_{layer_name}"
    # Replace non-alphanumeric characters with underscores
    layer_name = re.sub(r'\W+', '_', layer_name) 

    pattern = re.compile(r'Spatial Reference:\s*(\d+)')
    match = soup.find(string=pattern)
    crs = int(pattern.search(match).group(1))

    return layer_name, crs
def retrieve_raster_coords (soup, coord_name):
    """
    Retrieve a specific coordinate value from the HTML content using a regular expression.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML content.
        coord_name (str): The name of the coordinate value to be retrieved.

    Returns:
        str: The extracted coordinate value as a string.
    """ 
    # Define a regular expression pattern to find the coordinate values
    pattern = re.compile(fr'{coord_name}:\s*(-?\d+\.\d+)')
    # Find the first matching string in the HTML using the provided pattern
    match = soup.find(string=pattern)
    # Use the regular expression pattern to extract the coordinate value and return it as a string
    return pattern.search(match).group(1)
def raster_bbox (soup, crs, shp_bbox):
    """
    Check if the raster bounding box intersects with the shapefile bounding box.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML content.
        crs (str): The coordinate reference system (CRS) identifier of the raster.
        shp_bbox (geopandas.GeoSeries): The bounding box of the shapefile as a GeoSeries.

    Returns:
        bool: True if the raster bounding box intersects with the shapefile bounding box, False otherwise.
    """
    # Retrieve the raster bounding box coordinates from the soup HTML content
    xmin = retrieve_raster_coords (soup, 'XMin')
    ymin = retrieve_raster_coords (soup, 'YMin')
    xmax = retrieve_raster_coords (soup, 'XMax')
    ymax = retrieve_raster_coords (soup, 'YMax')
    # Create a polygon representing the raster bounding box
    polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
    # Create a GeoDataFrame to store the raster bounding box with the specified CRS
    data = {'Spatial_Reference': [crs], 'geometry': [polygon]}
    raster_gdf = gpd.GeoDataFrame(data, crs=crs)
    # Check if the raster bounding box intersects with the shapefile bounding box
    return raster_gdf.intersects(shp_bbox.unary_union)
def download_data (args):
    """
    Downloads data from the given URL, processes it, exports the extracted information to a GeoJSON file,
    clips the GeoJSON to the shapefile, and summarises the results in two spreadsheets for vectors and rasters.

    Args:
        args (tuple): A tuple containing the following elements:
            url (str): The URL of the webpage containing the data.
            shp (str): The path to the shapefile.
            export_path (str): The directory path where the exported data will be saved.
            shp_out_path (str): The directory path where the exported shapefile will be saved.

    Returns:
        None
    """
    # Unpack args
    url, shp, export_path, shp_out_path = args
    # Get the bounding box and CRS of the shapefile
    shp_gdf, shp_crs = shp_info (shp)
     # Send a GET request to the URL
    response = requests.get(url)
    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        if check_layer_type (soup) == 'Vector':
            # Extract and filter the layer name from the HTML content
            layer_name, crs = filter_layer_name_and_crs (soup)
            # Convert the geometry to the specified CRS and calculate the bounding box
            shp_bbox = list(shp_gdf.to_crs(crs).total_bounds)
            # Extract page data to GeoJSON
            geojson_out_path = run_esri2geojson (url, shp_bbox, crs, layer_name, export_path)
            # Clip the downloaded GeoJSON with the shapefile and save the clipped result as a shapefile
            clipped_status, out_path_shp = clip_geojson_export_shp (shp_crs, shp_gdf, geojson_out_path, shp_out_path)

            if not clipped_status:
                # Export information to sheets
                layer_name = os.path.splitext(os.path.basename(out_path_shp))[0]
                info_to_sheets (export_path, soup, layer_name, url, out_path_shp, 'vector')

        elif check_layer_type (soup) == 'Raster':
            # Extract and filter the layer name from the HTML content
            layer_name, crs = filter_layer_name_and_crs (soup)
            # Convert the geometry to the specified CRS and calculate the bounding box
            shp_bbox = shp_gdf.to_crs(crs).envelope

            if raster_bbox (soup, crs, shp_bbox).bool():
                out_path_shp = 'None'
                # Export information to sheets
                info_to_sheets (export_path, soup, layer_name, url, out_path_shp, 'raster')
        else:
            pass   
    else:
        print(url, "\nRequest failed with status code:", response.status_code)
def check_geojson (geojson_out_path, csv_path):
    """
    Check and update GeoJSON files based on a CSV file containing the list of filenames.

    Parameters:
        geojson_out_path (str): The path to the directory containing GeoJSON files.
        csv_path (str): The path to the CSV file containing the list of filenames.

    Returns:
        None
    """
    # Initialize a list to store the names of GeoJSON files found in the 'geojson_out_path'
    geojson_file_names = []
    # Loop through the files in the 'geojson_out_path' directory
    for filename in os.listdir(geojson_out_path):
        # Check if the file ends with the '.geojson' extension
        if filename.endswith('.geojson'):
            # If so, add the filename to the list
            geojson_file_names.append(filename)

    # Read the CSV file containing the list of filenames and store them in a set
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header if needed
        csv_filenames = {f'{row[1]}.geojson' for row in csv_reader}

    # Iterate through the GeoJSON files found earlier
    for filename in geojson_file_names:
        # Check if the filename is not present in the set of filenames from the CSV
        if filename not in csv_filenames:
            # If not, construct the full file path and remove the file from disk
            file_path = os.path.join(geojson_out_path, filename)
            os.remove(file_path)