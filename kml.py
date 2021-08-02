from zipfile import ZipFile
import os
import glob
import kml2geojson
import ee
import json

geometry_types = ['Polygon', 'Point', 'LineString']

datasets = ["LANDSAT/LC08/C01/T1"]


def main():
    ee.Initialize()
    kmz_file_paths = glob.glob("kmz\*")
    print(kmz_file_paths)
    geojson_file_paths = kmz_to_geojson(kmz_file_paths)
    ee_objects = geojson_to_earth_engine(geojson_file_paths)

#print(geometry_types)

def kmz_to_geojson(kmz_file_paths):
    '''
    Takes in the kmz file paths, unzips them to kml files, 
        then converts them to geojson files, 
        then returns the new geojson file locations in a list
    '''
    geojson_paths = []
    for path in kmz_file_paths:
        kmz = ZipFile(path, "r")
        # takes the last part of path without the .kmz. Replaced spaces with unscores if necessary
        filename = path.split("\\")[-1][:-4].replace(" ", "_")   
        print(filename)
        new_file_path = ""
        for i, name in enumerate(kmz.namelist()):
            if i > 0:
                new_file_path = os.path.join("kml", filename + f"{i}.kml")
            else:
                new_file_path = os.path.join("kml", filename + ".kml")
            with open(new_file_path, 'wb') as kml_file:
                kml_file.write(kmz.open(name, 'r').read())
            print(name)
            kml2geojson.main.convert(new_file_path, 'geojson')
            geojson_path = f"geojson\\{filename}.geojson"
            geojson_paths.append(geojson_path)
    return geojson_paths


def geojson_to_earth_engine(geojson_file_paths):
    '''
    Takes in list file paths for geojson files
    Returns a list of earth engine objects, where each element of the list
        represents the feature or collection of features for each geojson file
    '''
    ee_objects = []
    for geojson_path in geojson_file_paths:
        with open(geojson_path, 'r') as json_file:
            geojson_dict = json.loads(json_file.read())            
            feature_type, feature_obj = geojson_feature_parser(geojson_dict)
            ee_objects.append(feature_obj)
    print(len(ee_objects))
    return ee_objects

             
def geojson_feature_parser(gj):
    '''
    Takes in full initial geojson and parses it into earth engine types:
        FeatureCollection
        Feature
    Returns object in the form: feature_type : str, feature_object : ee object
    '''
    if type(gj) is not dict:
        print(f"json structure is not a dict it is a {str(type(gj))}")
        return None
    if gj["type"] == "FeatureCollection":
        feature_list = []
        for json_feature in gj["features"]: # assumes it has the features member
            if json_feature["type"] != "Feature":
                raise Exception("geojson_feature_parser: not a feature type: " + json_feature["type"])
                continue
            feature_type, feature_obj = geojson_feature_parser(json_feature)
            # can check if feature_type is "Feature", but likely unecessary
            feature_list.append(feature_obj)
        return "FeatureCollection", ee.FeatureCollection(feature_list)
    elif gj["type"] == "Feature":
        # Need to turn into Feature object
        geometry_type, geometry_obj = geojson_geometry_parser(gj["geometry"])
        # type not used now, but might be useful
        return "Feature", geometry_obj
    else:
        raise Exception("geojson_feature_parser: unhandled type: " + gj["type"])


def geojson_geometry_parser(geometry):
    ''' 
    Parses the geometry types from geojson into google earth objects
    works for types: ['Polygon', 'Point', 'LineString']

    issue: coordinates might need to be in xy not xyz
    '''
    if not geometry["type"]:
        raise Exception("Not a geometry type")
    if geometry["type"] == "Polygon":
        if len(geometry["coordinates"]) != 1:
            raise Exception("Zero polygons or more than one in polygon type")
        coordinates = geometry["coordinates"][0]
        new_coordinates = remove_altitude(coordinates)
        polygon = ee.Geometry.Polygon(new_coordinates)
        return "Polygon", polygon
        # write_to_file("geometry.txt", str(geometry["coordinates"]))
        # ee.Geometry.Polygon(coords, proj, geodesic, maxError, evenOdd)
        # geometry[coordinates] is a list of polygon coordinate lists, usually just one
        # geometry[coordinates][0] is a  list of many coordinates for the polygon
    elif geometry["type"] == "Point":
        coordinates = geometry["coordinates"]
        if len(coordinates) == 3:
            new_coordinates = [coordinates[0], coordinates[1]]
        else:
            raise Exception()("Point coordinates not in 3d: " + str(coordinates))
        point = ee.Geometry.Point(new_coordinates)
        return "Point", point
        # ee.Geometry.Point(coords, proj)
        # coords	List	A list of two [x,y] coordinates in the given projection.
        # geometry[coordinates] is a set of xyz coordinates
    elif geometry["type"] == "LineString":
        # ee.Geometry.LineString(coords, proj, geodesic, maxError)
        # geometry["coordinates"] is a list of line coordinate sets
        # geometry["coordinates"][0] is a set of xyz coordinates
        coordinates = geometry["coordinates"]
        new_coordinates = remove_altitude(coordinates)
        line_string = ee.Geometry.LineString(new_coordinates)
        return "LineString", line_string
    else:
        raise Exception(f"geojson_geometry_parser: unhandled geometry type: " + str(geometry["type"]))
    
    return(geometry["type"])


def remove_altitude(coordinates):
    '''
    Takes in a list of 3d coordinates and returns a list of 2d coordinates
    (Could just replace with list comprehension)
    '''
    new_coords = []
    for xyz in coordinates:
        if len(xyz) == 3:
            new_coords.append([xyz[0], xyz[1]])
        else:
            raise Exception("Not all coordinates are three dimensional")
    return new_coords


def write_to_file(filename, text):
    with open(filename, "w") as f:
        f.write(text)


if __name__ == "__main__":
    main()