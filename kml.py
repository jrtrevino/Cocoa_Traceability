from zipfile import ZipFile
import os
import glob
import kml2geojson
import ee
import json

geometry_types = ['Polygon', 'Point', 'LineString']

def main():
    ee.Initialize()
    kmz_file_paths = glob.glob("polygons\*")
    for path in kmz_file_paths:
        kmz = ZipFile(path, "r")
        filename = path.split("\\")[-1][:-4].replace(" ", "_")   # takes the last part of path without the .kmz
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
            with open(geojson_path, 'r') as json_file:
                json_dict = json.loads(json_file.read())
                print(list(json_dict))
                print(f"\n ^^ {filename} list json_dict ^^")
                
                parsed = geojson_feature_parser(json_dict)
                print("parsed:")
                print(parsed)

                # -----------------------------------------
                # test = ee.FeatureCollection(json_dict)
                # print(test)
                # print(f"\n ^^ {filename} geometry ^^")
    print(geometry_types)
            
                
def geojson_feature_parser(gj):
    if type(gj) is not dict:
        print(f"json structure is not a dict it is a {str(type(gj))}")
        return None
    if gj["type"] == "FeatureCollection":
        feature_list = []
        for feature in gj["features"]: # assumes it has the features member
            if feature["type"] != "Feature":
                print("Not feature type")
                continue
            feature_list.append(geojson_feature_parser(feature))
        # want to return list of Feature objects as a FeatureCollection object
        # for now just returning list as a list
        return feature_list
    elif gj["type"] == "Feature":
        # Need to turn into Feature object
        return geojson_geometry_parser(gj["geometry"])


def geojson_geometry_parser(geometry):
    ''' 
    Parses the geometry types from geojson into google earth objects
    works for types: ['Polygon', 'Point', 'LineString']
    '''
    if not geometry["type"]:
        raise Exception("Not a geometry type")
    if geometry["type"] == "Polygon":
        # ee.Geometry.Polygon(coords, proj, geodesic, maxError, evenOdd)
        # geometry[coordinates] is a list of polygon coordinate lists, usually just one
        # geometry[coordinates][0] is a  list of many coordinates for the polygon
        pass
    if geometry["type"] == "Point":
        # ee.Geometry.Point(coords, proj)
        # coords	List	A list of two [x,y] coordinates in the given projection.
        # geometry[coordinates] is a set of xyz coordinates
        pass
    if geometry["type"] == "LineString":
        # ee.Geometry.LineString(coords, proj, geodesic, maxError)
        # geometry["coordinates"] is a list of line coordinate sets
        # geometry["coordinates"][0] is a set of xyz coordinates
        pass
    
    return(geometry["type"])
    # if geometry["type"] == "Polygon":
    #     pass
    # elif geometry["type"] == ""


if __name__ == "__main__":
    main()

# types = {
#     "FeatureCollection":"List of feature objects, possibly more folders, will come with a \"features\" folder"
#     "Feature":""
# }