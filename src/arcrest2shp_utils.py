import os
import geopandas as gpd
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

def bbox_shp (shp, crs):
    # Read the shapefile using geopandas
    gdf = gpd.read_file(shp)
    # # Read the shapefile Coordinate reference system (CRS) code
    # gdf = gdf.to_crs(crs)   
    # Convert the geometry to the specified CRS and calculate the bounding box
    bbox = list(gdf.to_crs(crs).total_bounds)

    return bbox
    
def run_esri2geojson (url, bbox, crs, layer_name, export_path):
    """
    Runs the 'esri2geojson' command-line tool to convert data from an Esri service to GeoJSON format.

    Args:
        url (str): The URL of the Esri service.
        bbox (list): A list of four float values representing the bounding box coordinates in the order [minx, miny, maxx, maxy].
        crs (str): The coordinate reference system (CRS) identifier.
        gjson_out_path (str): The file path for the output GeoJSON file.

    Returns:
        None

    Raises:
        subprocess.CalledProcessError: If the 'esri2geojson' command fails to execute.
    """
    geojson_out_path = os.path.join(export_path, 'geojson', f'{layer_name}.geojson')

    count = 0
    while os.path.exists(geojson_out_path):
        count += 1
        geojson_out_path = os.path.join(
            export_path, 'geojson', f'{layer_name}_{str(count)}.geojson'
        )

    # Edit geometry for input
    geometry_str = ''.join(['geometry=', ','.join([str(num) for num in bbox])])
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
        print(f'{url} An error occurred while executing the run_esri2geojson.')

    return geojson_out_path

def clip_geojson_export_shp (shp, geojson_out_path, shp_out_path):
    """
    Clips a GeoJSON file using a polygon GeoDataFrame and exports the clipped data to a shapefile.

    Args:
        aoi_gdf (GeoDataFrame): A GeoDataFrame representing the area of interest polygon for clipping.
        gjson_out_path (str): The file path of the input GeoJSON file.

    Returns:
        None
    """
    # Suppress the UserWarning
    warnings.filterwarnings("ignore", message="Column names longer than 10 characters will be truncated when saved to ESRI Shapefile.")

    # Read the GeoJSON and shapefile into GeoDataFrames   
    geojson_gdf = gpd.read_file(geojson_out_path)
    # Check if the GeoJSON GeoDataFrame is empty

    if not geojson_gdf.empty:
        # Read the shapefile into a GeoDataFrame
        shp_gdf = gpd.read_file(shp)
        # Read the shapefile CRS 
        crs_shp = shp_gdf.crs
        
        # Convert the GeoJSON GeoDataFrame to the shapefile coordinate reference system (CRS)
        geojson_gdf = geojson_gdf.to_crs(crs_shp)

        # Clip the GeoJSON with the shapefile
        clipped_gdf = gpd.clip(geojson_gdf, shp_gdf)

        # Check if the clipped GeoDataFrame is not empty
        if not clipped_gdf.empty:
            # Extract the file name without extension
            output_shapefile = f'{os.path.splitext(os.path.basename(geojson_out_path))[0]}.shp'
            # Export the clipped GeoDataFrame to a shapefile
            clipped_gdf.to_file(os.path.join(shp_out_path, output_shapefile))

def check_if_vector (soup):
    """
    Check if the HTML page contains information about a vector geometry type.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.

    Returns:
        bool: True if the page contains vector geometry type information, False otherwise.
    """
    # Find all tags that contain the text "esriGeometry"
    cells = soup.find_all(lambda tag: tag.name == 'b' and 'Geometry Type:' in tag.text)
    # Check if any cell contains the desired text
    contains_esriGeometry = any('esriGeometry' in cell.next_sibling.strip() for cell in cells)

    if contains_esriGeometry:
        return True
    else:
        return False

def info_to_sheets (export_path, soup, layer_name, url):
    """
    Write extracted information to a CSV file.

    Args:
        export_path  (str): The output directory path.
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.
        layer_name (str): The name of the layer.
        url (str): The URL of the layer.
    """
    # Define the file path
    file_path = os.path.join(export_path, 'extracted_data.csv')
    # Extract the geometry type from the soup object
    geometry_type = soup.find('b', string='Geometry Type:').next_sibling.strip()
    # Extract the description text from the soup object
    description_text = soup.find('b', string='Description: ').next_sibling.strip()
    # Extract the source from the layer_name by splitting it at underscores and taking the first part
    source = layer_name.split('_')[0]
    # Get the current date
    extraction_date = datetime.date.today()
    # Create a list containing the extracted data
    data = [source, layer_name, geometry_type, description_text, url, extraction_date]
    # Write the extracted data to the CSV file
    write_csv (file_path, data)

def create_csv (export_path):
    # Define the file path
    csv_path = os.path.join(export_path, 'extracted_data.csv')
    #define columns
    columns = ['Source', 'Name', 'Geometry Type', 'Description', 'URL', 'Extraction Date']
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
    Filter and format the layer name extracted from the HTML soup.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.

    Returns:
        str: The filtered and formatted layer name.
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

def download_data (url, shp, export_path):
    """
    Downloads data from the given URL, processes it, and exports the extracted information to a GeoJSON file.

    Args:
        url (str): The URL of the webpage containing the data.
        shp (str): The path to the shapefile.
        export_path (str): The directory path where the exported data will be saved.

    Returns:
        str: The path to the exported GeoJSON file.

    Raises:
        None
    """
    # Send a GET request to the URL
    response = requests.get(url)
    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find all <li> elements in the parsed HTML
        for li in soup.find_all('li'):
             # If the <li> element contains <a> tags, skip processing
            if li.find_all('a'):
                pass

            elif check_if_vector (soup):
                # Extract and filter the layer name from the HTML content
                layer_name, crs = filter_layer_name_and_crs (soup)
                # Export information to sheets
                info_to_sheets (export_path, soup, layer_name, url)
                # Get the bounding box and CRS of the shapefile
                bbox = bbox_shp (shp, crs)
                # Extract page data to GeoJSON
                geojson_out_path = run_esri2geojson (url, bbox, crs, layer_name, export_path)
                break
            else:
                pass     
        return geojson_out_path
    else:
        print(url, "\nRequest failed with status code:", response.status_code)

def check_csv_entries(csv_path, shapefile_folder_path):
    shapefile_names = []
    for filename in os.listdir(shapefile_folder_path):
        if filename.endswith(".shp"):  # Assuming shapefiles have the .shp extension
            shapefile_names.append(filename[:-4])

    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(csv_path)  
    # Keep only the rows where the file name is present in the shapefile names
    df = df[df['Name'].isin(shapefile_names)]
    # Write the filtered DataFrame back to the CSV file
    df.to_csv(csv_path, index=False)