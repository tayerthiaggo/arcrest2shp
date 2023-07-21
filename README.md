# ArcREST2shp

ArcREST2SHP allows you to access an ArcGIS REST Services Directory, iterate through all nested URLs under the URL provided, extract GeoJSON data (using [esri-dump](https://github.com/openaddresses/pyesridump)), convert it into shapefiles, and generate a CSV file with all the extracted data. 

## Clone the Repository
To get started, you can clone this repository to your local machine using Git. Follow the steps below to clone the repository:

1. Open a terminal or command prompt on your computer.

2. Change the current working directory to the location where you want to clone the repository.

3. Use the following command to clone the repository:

```bash
git clone https://github.com/tayerthiaggo/arcrest2shp.git
```

## Requirements
- Python 3.x installed on your system.
- Additionally, install the required dependencies by running the following command:

```bash
pip install -r requirements.txt
```

## Example
```python
from arcrest2shp import arcrest2shp

# Call the function with appropriate arguments
url_base = "https://example.com/data/"
shp = "path/to/shapefile.shp"
out_path = "output_folder/"
num_threads = 10

arcrest2shp(url_base, shp, out_path, crs, num_threads)
```
In this example, data will be downloaded from the URL "https://example.com/data/" and saved as shapefiles in the "output_folder/" after clipping with the specified shapefile. The output files CRS will be the same as the input shapefile, and the script will use ten threads for concurrent processing.

## How it works
1. Downloading data: The script explores the provided url_base and all nested URLs under it and retrieves all links containing spatial data (vectors). The data is downloaded as GeoJSON files.
2. Clipping and exporting: The downloaded GeoJSON files are clipped using the provided shapefile (shp). The resulting clipped data is saved as shapefiles with the same CRS as the input shapefile.
3. Concurrent processing: The script utilizes concurrent processing using ThreadPoolExecutor with the number of threads specified by the num_threads parameter. This accelerates the data retrieval and conversion process.
4. Folder and file organization: The script creates the necessary data storage folders. It organizes the downloaded data in the output folder with separate folders for GeoJSON and shapefiles and generates a CSV file with all the extracted data.

## Notes
Ensure that the arcrest2shp_utils module is available in the same directory as the script or in the Python environment where you execute the script.
Depending on the volume of data and your system's capabilities, you may adjust the num_threads parameter to optimize performance.

Please ensure you have the necessary permissions to access the ArcGIS REST Services Directory and the data it contains. Modify the input parameters in the Jupyter Notebook example to suit your specific use case.
