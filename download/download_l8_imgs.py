import json
import time
import os
import argparse
from datetime import datetime
from pathlib import Path

from util import *


# --------------------------------------------------------------------------- #
# Code adapted from UGSS M2M API sample script
# https://m2m.cr.usgs.gov/api/docs/example/download_landsat_c2-py
# --------------------------------------------------------------------------- #


""" Log in to the API and return the API key to use with future requests. """
def authenticate(url, username=None, password=None, verbose=True):
    # To access the API, first register for USGS Eros credentials here: https://ers.cr.usgs.gov/register
    # Next, log in and request access to the M2M API here: https://ers.cr.usgs.gov/profile/access
    # It might take a few days to be granted access.
    if not username or not password:
        username, password = get_credentials("USGS Eros Username: ")
    payload = {'username': username, 'password': password}
    if verbose: print("Logging in...")
    api_key = send_request(url + "login", payload, verbose=verbose)
    return api_key


""" Search for images matching a certain criteria, and return a list of products that
    can be downloaded. date_range and cloud_range should be tuples, and boundary should
    point to a GeoJSON file. Reserving **kwargs to be used in the future if needed. """
def search(url, api_key, dataset=None, max_results = 10, date_range=None, cloud_range=None, boundary=None, verbose=True, **kwargs):
    if verbose: print("Fetching scenes...")
    
    # search for scenes that match our criteria
    acquisition_filter = cloud_cover_filter = ingest_filter = metadata_filter = seasonal_filter = spatial_filter = None
    
    if date_range:
        acquisition_filter = {
            'start': date_range[0],
            'end': date_range[1]
        }
        
    if cloud_range:
        cloud_cover_filter = {
            'min': cloud_range[0],
            'max': cloud_range[1],
            'includeUnknown': True
        }
    
    if boundary:
        with open(boundary) as file:
            geojson = format_geojson(file)
        
        spatial_filter = {
            'filterType': 'geojson',
            'geoJson': geojson
        }
            
    scene_filter = {
        'acquisitionFilter': acquisition_filter,
        'cloudCoverFilter': cloud_cover_filter,
        'datasetName': dataset,
        'ingestFilter': ingest_filter,
        'metadataFilter': metadata_filter,
        'seasonalFilter': seasonal_filter,
        'spatialFilter': spatial_filter
    }
    
    payload = { 
        'datasetName': dataset,
        'maxResults': max_results,
        'startingNumber': 1, 
        'sceneFilter': scene_filter
    }
    
    matching_scenes = send_request(url + "scene-search", payload, api_key, verbose=verbose)['results']
    entityIds = []
    for scene in matching_scenes:
        entityIds.append(scene['entityId'])
    
    # add scenes to list
    list_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        'listId': list_id,
        'entityIds': entityIds,
        'datasetName': dataset
    }
    
    num_scenes = send_request(url + "scene-list-add", payload, api_key, verbose=verbose)   
    
    # get download options
    payload = {
        'listId': list_id,
        'datasetName': dataset
    }
    
    products = send_request(url + "download-options", payload, api_key, verbose=verbose)
    downloads = []
    for product in products:
        if product['bulkAvailable']:         
            downloads.append({'entityId':product['entityId'], 'productId':product['id']})
    
    # remove the list
    payload = {
        'listId': list_id
    }
    
    send_request(url + "scene-list-remove", payload, api_key, verbose=verbose) 
    
    return downloads


class JSONFormatError(Exception):
    pass


""" Translate geojson into a format that the API understands. """
def format_geojson(geojson):
    geojson = dict(json.load(geojson))  
    try:
        if geojson['type'] == 'FeatureCollection':
            if len(geojson['features']) != 1:
                raise JSONFormatError(("Unsupported number of GeoJSON features: {len(geojson['features'])}."))
            geometry = geojson['features'][0]['geometry']
        elif geojson['type'] == 'Feature':
            geometry = geojson['geometry']
        else:
            raise JSONFormatError(f"Unsupported GeoJSON type: {geojson['type']}")

        formatted = {}
        formatted['type'] = geometry['type']
        formatted['coordinates'] = geometry['coordinates']
        return formatted
    except KeyError as e:
        raise JSONFormatError(f"Error parsing JSON: missing {e} field")
        

def download(url, api_key, downloads, completed_list=[], verbose=True):
    if verbose:
        download = input(f"Download all {len(downloads)} scene(s)? (Y/N) ")
        if download.lower() not in {'y', 'yes'}:
            return None
        
    if verbose: print("Requesting scenes for download...")
    label = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        'downloads': downloads,
        'label': label
    }
    
    results = send_request(url + "download-request", payload, api_key, verbose=verbose)
    
    for result in results['availableDownloads']:       
        threaded_download(result['url'], threads, completed_list, verbose=verbose)
    
    # if downloads not immediately available, poll until they become available
    preparing_dl_count = len(results['preparingDownloads'])
    preparing_dl_ids = []
    if preparing_dl_count > 0:
        if verbose: print("Waiting for all downloads to become available...")
        for result in results['preparingDownloads']:  
            preparing_dl_ids.append(result['downloadId'])
        payload = {"label" : label}   
        while len(preparing_dl_ids) > 0: 
            time.sleep(30)
            results = send_request(url + "download-retrieve", payload, api_key, False, verbose=verbose)
            if results != False:
                for result in results['available']:                            
                    if result['downloadId'] in preparing_dl_ids:
                        preparing_dl_ids.remove(result['downloadId'])
                        threaded_download(result['url'], threads, completed_list, verbose=verbose)
                        
                for result in results['requested']:   
                    if result['downloadId'] in preparing_dl_ids:
                        preparing_dl_ids.remove(result['downloadId'])
                        threaded_download(result['url'], threads, completed_list, verbose=verbose)
                        
    return completed_list


def process_metadata(folder):
    # TODO: what metadata files do we actually care about?
    metadata_files = ['ANG', 'MTL.txt']
    return get_matching_files(folder, metadata_files) 


def main():
    parser = argparse.ArgumentParser(
        description='Search for and download L8 scenes that match criteria.')
    
    parser.add_argument('-date-range', '--dr', metavar=('start', 'end'), 
                        dest='date_range', nargs=2, type=str,
                        help='filter scenes by acquisition date (format: yyyy-mm-dd yyyy-mm-dd)')
    parser.add_argument('-cloud-range', '--cr', metavar=('min', 'max'),
                        dest='cloud_range', nargs=2, type=int,
                        help='filter scenes by cloud cover')
    parser.add_argument('-boundary', '--b', metavar='geojson',
                        dest='boundary', type=Path,
                        help="path to geojson file with boundary of search")
    args = parser.parse_args()
    
    url = "https://m2m.cr.usgs.gov/api/api/json/stable/"
    dataset = "landsat_ot_c2_l2"
    bucket = ""
    prefix = ""
    # TODO: add verbosity flag as argument
    verbose = True
    
    api_key = authenticate(url, verbose=verbose)
    
    downloads = search(url, api_key, dataset,  
                        date_range=args.date_range, 
                        cloud_range=args.cloud_range, 
                        boundary=args.boundary,
                        verbose=verbose)
    
    completed_list = []
    download(url, api_key, downloads, completed_list, verbose=verbose)
    
    # wait until all downloads have finished
    for thread in threads:
        while thread.is_alive():
            time.sleep(30)
    
    # unzip each file and generate tif, qa, and metadata files for each to be uploaded to s3
    for file in completed_list:
        # TODO: make this a function?
        
        if verbose: print(f"Extracting {file}...")
        unzipped = extract(file, delete=True)
        
        if verbose: print(f"Calculating NDVI for {unzipped}...")
        ndvi = calc_ndvi('B4', 'B5', unzipped)
        
        # if verbose: print(f"Building VRTs and generating metadata files for {unzipped}...")
        # select B, G, R, and NIR bands
        rgbnir_bands = ['B2', 'B3', 'B4', 'B5']
        rgbnir_band_filenames = get_matching_files(unzipped, rgbnir_bands)
        # rgbnir_name = f"{unzipped}{os.path.sep}rgbnir.vrt"
        # rgbnir_band_filenames = build_vrt(rgbnir_name, unzipped, rgbnir_bands)
        
        # # select all QA bands
        # # TODO: what QA bands do we actually need?
        qa_bands = ['QA']
        qa_band_filenames = get_matching_files(unzipped, qa_bands)
        # qa_name = f"{unzipped}{os.path.sep}qa.vrt"
        # qa_band_filenames = build_vrt(qa_name, unzipped, qa_bands)
        
        metadata_filenames = process_metadata(unzipped)
        
        if verbose: print("Deleting extra files...")
        keep_files = []
        keep_files.extend(rgbnir_band_filenames)
        keep_files.extend(qa_band_filenames)
        keep_files.extend(metadata_filenames)
        keep_files.extend([ndvi])
        # keep_files.extend([rgbnir_name, qa_name])
        all_files = [f"{unzipped}{os.path.sep}{f}" for f in os.listdir(unzipped)]
        delete_files = [f for f in all_files if f not in keep_files]
        for file in delete_files:
            os.remove(file)
        
        TODO: upload to s3

    if verbose: print("Done.")
    

if __name__ == "__main__":
    main()