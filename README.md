# data_wa_extractor

A lightweight Python (tested in Python 3.x and 2.x) module that scrapes ESRI REST endpoints and parses the data into a local or enterprise geodatabase feature class

A library for retrieving features from an ArcGIS FeatureServer or MapServer. This library makes tiled requests rather than simply requesting every feature.

Scrapes an Esri REST endpoint and writes a GeoJSON file

uses esri-dump
https://github.com/openaddresses/pyesridump


Scraping Geographic Features from ArcGIS Server

Still many geographic data is delivered through ESRI’s ArcGIS Server. It is not easy to utilize the geographic data in the GIS servers from data analysis platform like R or Pandas. This package enables users to scrape vector data in ArcGIS Server from R through the server’s REST API.


How it works
This program sends a request to an ArcGIS Server and gets json responses containing coordinates of geometries of which format is not the same as geojson. So it converts the json into simple feature geometries from the response. Then it combines attribute data to the geometries to create sf dataframe. Often ArcGIS servers limits the maximum number of rows in the result set. So this program creates 500 features per request and automatically re-send requests until it gets all features in the dataset.


How to use
What you need is the URL of REST service you want.
