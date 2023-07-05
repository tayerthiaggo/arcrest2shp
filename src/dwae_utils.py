
import os
import geopandas as gpd
import subprocess
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import csv
import datetime


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
        os.makedirs(export_path)
        print(f"Folder created: {export_path}")
    except:
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

    # Send a GET request to the URL
    response = requests.get(url)
    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <li> elements
        li_tags = soup.find_all('li')

        links = []

        for li in li_tags: 
                    
            if li.find_all('a'):
                # has_links = True
                # Find all <a> elements within the <li>
                a_tags = li.find_all('a')
                for a in a_tags:            
                    # Extract the link URL from the <a> element
                    link = urljoin(url, a.get('href'))
                    links.append(link)    
        return links
    else:
        print(url, "\nRequest failed with status code:", response.status_code)
        
        return []

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

def bbox_shp (shp, crs=7844):
    """
    Get the bounding box coordinates of a shapefile.

    Args:
        shp (str): Path to the shapefile.
        crs (int, optional): Coordinate reference system (CRS) code. Default is 7844.

    Returns:
        tuple: A tuple containing the bounding box coordinates (list) and the CRS code (int).

    """
    
    # Read the shapefile using geopandas
    gdf = gpd.read_file(shp)

    # Convert the geometry to the specified CRS and calculate the bounding box
    bbox = list(gdf.to_crs(crs).total_bounds)

    return bbox, crs
    # return json.dumps(geometry_dict), crs

    
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

    Example:
        url = 'https://services.slip.wa.gov.au/public/rest/services/SLIP_Public_Services/Boundaries/MapServer/2'
        bbox = [115.8444528, -31.98380876, 116.15245686, -31.70508152]
        crs = '4326'
        gjson_out_path = 'E:\Scripts\idot_roads5.geojson'
        run_esri2geojson(url, bbox, crs, gjson_out_path)
    """
    
    geojson_out_path = os.path.join(export_path, 'geojson', f'{layer_name}.geojson')

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

def clip_geojson_export_shp (shp, crs,  geojson_out_path, shp_out_path):
    """
    Clips a GeoJSON file using a polygon GeoDataFrame and exports the clipped data to a shapefile.

    Args:
        aoi_gdf (GeoDataFrame): A GeoDataFrame representing the area of interest polygon for clipping.
        gjson_out_path (str): The file path of the input GeoJSON file.

    Returns:
        None

    Example:
        import geopandas as gpd

        # Define the area of interest as a polygon GeoDataFrame
        aoi_polygon = gpd.read_file('path/to/aoi.shp')

        # Specify the input GeoJSON file
        input_gjson = 'path/to/input.geojson'

        # Call the function to clip and export
        clip_geojson_export_shp(aoi_polygon, input_gjson)
    """

    # Read the GeoJSON and shapefile into GeoDataFrames
    geojson_gdf = gpd.read_file(geojson_out_path).to_crs(crs)
    
    if geojson_gdf.empty:
        pass
    else:
        shp_gdf = gpd.read_file(shp).to_crs(crs)

        # Clip the GeoJSON with the shapefile
        clipped_gdf = gpd.clip(geojson_gdf, shp_gdf)

        # Shorten the column names to a maximum of 10 characters
        clipped_gdf = clipped_gdf.rename(columns=lambda x: x[:10])

        # Extract the file name without extension
        output_shapefile = f'{os.path.splitext(os.path.basename(geojson_out_path))[0]}.shp'

        # Export the clipped GeoDataFrame to a shapefile
        clipped_gdf.to_file(os.path.join(shp_out_path, output_shapefile), driver='ESRI Shapefile')

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
        out_path (str): The output directory path.
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

    source = '_'.join(layer_name.split('_')[0:1])

    # Get the current date
    extraction_date = datetime.date.today()

    data = [source, layer_name, geometry_type, description_text, url, extraction_date]

    write_csv (file_path, data)


def create_csv (export_path):

    # Define the file path
    file_path = os.path.join(export_path, 'extracted_data.csv')

    #define columns
    columns = ['Source', 'Name', 'Geometry Type', 'Description', 'URL', 'Extraction Date']
    # Check if the file exists
    file_exists = os.path.isfile(file_path)
    # Write the extracted information to the CSV file
    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row if the file is newly created
        if not file_exists:
            writer.writerow(columns)


def write_csv (file_path, data):
    # Check if the file exists
    file_exists = os.path.isfile(file_path)

    # Write the extracted information to the CSV file
    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write the new row of data
        writer.writerow(data)

def filter_layer_name (soup):
    """
    Filter and format the layer name extracted from the HTML soup.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.

    Returns:
        str: The filtered and formatted layer name.

    """

    # Retrieve the <h2> name ('Layer')
    h2_tag = soup.find('h2')
    layer_name = h2_tag.text.split(':')[1].split('(ID')[0].replace(')', '').strip()

    # Check if the page has a 'Parent Layer' section
    parent_layer_tag = soup.find('b', string='Parent Layer:')
    if parent_layer_tag:
        layer_name = h2_tag.text.split(':')[1].split('(')[0].strip()
        parent_layer_link = parent_layer_tag.find_next_sibling('a')
        if parent_layer_link:
            parent_layer_name = parent_layer_link.text.split('(')[1].split(')')[0].strip()
            layer_name = parent_layer_name + ' ' + layer_name

    # Replace non-alphanumeric characters with underscores
    layer_name = re.sub(r'\W+', '_', layer_name)

    layer_name = f"{'_'.join(layer_name.split('_')[-2:])}_{'_'.join(layer_name.split('_')[:-2])}"

    return layer_name

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

        # Find all <li> elements
        for li in soup.find_all('li'):

            if li.find_all('a'):
                pass

            elif check_if_vector (soup):

                layer_name = filter_layer_name (soup)
                  
                info_to_sheets (export_path, soup, layer_name, url)

                bbox, crs = bbox_shp (shp)

                geojson_out_path = run_esri2geojson (url, bbox, crs, layer_name, export_path)

                break

            else:
                pass
        
        return geojson_out_path
    else:
        print(url, "\nRequest failed with status code:", response.status_code)


# def process_url(url):
#     print('i')
#     geojson_out_path = download_data(url, shp, export_path)
#     clip_geojson_export_shp(shp, crs, geojson_out_path, shp_out_path)