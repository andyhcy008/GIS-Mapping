import numpy as np
import pandas as pd
import shapefile
from osgeo import gdal
from shapely.geometry import MultiPoint, Point, Polygon,shape
from shapely.geometry.polygon import Polygon


shapefile1 = shapefile.Reader(r"~\Project\Mapping\PLCHC_Adis\PLCHC_Adis.shp")   # Load shapefile
dt = pd.read_csv(r"~\Project\Mapping\hamilton\hamilton.csv")    # Load csv file

all_shapes = shapefile1.shapes()   # Get shape from file
all_records = shapefile1.records()  # Get coordinates from file


dt['coordinates'] = list(zip(dt.LON, dt.LAT))   # Merge Lontitude and Latitude to one tuple column



def find_coordinates(row):
    '''
    iterate each row of 'coordinates' in dataframe
    
    return the related name of location
    
    '''
    for i in range(len(all_shapes)):
        boundary = all_shapes[i]
        if Point(row).within(shape(boundary)):
            name = all_records[i][1]
            return name
            
            
dt['LocationName'] = dt['coordinates'].apply(lambda x: find_coordinates(x))
