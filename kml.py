from zipfile import ZipFile
import os
import glob
import kml2geojson
import ee
import json


datasets = ["LANDSAT/LC08/C01/T1"]
username = "bmiche01"

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
        filename = ""
        if len(path.split("/")) == 1:
            filename = path.split("\\")[-1][:-4].replace(" ", "_")
        else:
            filename = path.split("/")[-1][:-4].replace(" ", "_")   
        new_file_path = ""
        for i, name in enumerate(kmz.namelist()):
            if i > 0:
                new_file_path = os.path.join("kml", filename + f"{i}.kml")
            else:
                new_file_path = os.path.join("kml", filename + ".kml")
            with open(new_file_path, 'wb') as kml_file:
                kml_file.write(kmz.open(name, 'r').read())
            kml2geojson.main.convert(new_file_path, 'geojson')
            geojson_path = os.path.join("geojson", f"{filename}.geojson")
            print("Generated " + geojson_path)
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
        geometry_type, geometry = geojson_geometry_parser(gj["geometry"])
        properties = gj["properties"]
        return "Feature", ee.Feature(geometry, properties) 
        # type not used now, but might be useful
    else:
        raise Exception("geojson_feature_parser: unhandled type: " + gj["type"])


def geojson_geometry_parser(geometry):
    ''' 
    Turns GeoJSON geometry into an ee.Geometry() object.
    Our kmz files inlcude elevation data for each coordinate, which GEE does
    not seem to support. Consequently, we must remove this data before
    creating the geometry object.
    '''
    geometry["coordinates"] = remove_altitude(geometry["coordinates"])
    # some of the LineStrings only have one point, GEE will throw an error at this
    if geometry["type"] == "LineString" and len(geometry["coordinates"]) < 2:
        geometry["type"] = "Point"
        geometry["coordinates"] = geometry["coordinates"][0]
    return geometry["type"], ee.Geometry(geometry)


def remove_altitude(coordinates):
    '''
    Given either a coordinate or a list of coordinates, recursively strip the
    elevation data from each coordinate.
    GeoJSON spec defines a coordinate as a list containing [long, lat, elevation (optional)].
    GEE does not seem to support elevation data, so we must strip it before creating
    GEE objects.
    '''
    stripped_coords = []
    # first, check if we are dealing with a coordinate or list of coordinates
    if type(coordinates[0]) is list:
        for coord in coordinates:
            stripped_coords.append(remove_altitude(coord))
    else:
        # if there is an elevation value, remove it
        if len(coordinates) == 3:
            stripped_coords = coordinates[0:2]
        else:
            stripped_coords = coordinates
    return stripped_coords



def export_ee_assets(ee_obj, name):
    '''
    Given a GEE object in memory and a name, uploads it to GEE so you can access it in the
    code editor.
    NOTE: change {username} to your Google username.
    '''
    name = os.path.splitext(name)[0].split("/")[-1] # remove extension and folder
    task = ee.batch.Export.table.toAsset(collection=ee_obj, description=name, assetId=("users/{username}/" + name))
    task.start()
    print("Uploading " + name + " to GEE...")


def main():
    ee.Initialize()
    kmz_file_paths = glob.glob("kmz/*")
    print(kmz_file_paths)
    # TODO: only need to generate geojson files once, maybe split into separate script
    print("Converting kmz files to geoJSON files...")
    geojson_file_paths = kmz_to_geojson(kmz_file_paths)
    print("Generating GEE objects...")
    ee_objects = geojson_to_earth_engine(geojson_file_paths)
    for obj, name in zip(ee_objects, geojson_file_paths):
        export_ee_assets(obj, name)
    
    
if __name__ == "__main__":
    main()