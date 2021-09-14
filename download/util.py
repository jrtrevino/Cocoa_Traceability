import getpass
import json
import requests
import sys
import threading
import re
import os
import zipfile
import tarfile
import glob

import numpy as np
from osgeo import gdal
import boto3
from botocore.exceptions import ClientError

# TODO: remove these hardcoded variables
maxthreads = 5 # threads count for downloads
sema = threading.Semaphore(value=maxthreads)
threads = []
path = "" # download path


""" Prompt user for API credentials. """
def get_credentials(prompt="Username: "):
    username = input(prompt)
    password = getpass.getpass("Password: ")
    return username, password


""" Send an HTTP POST request. """
def send_request(url, data, apiKey=None, exitIfNoResponse=True, verbose=True):  
    json_data = json.dumps(data)
    
    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}              
        response = requests.post(url, json_data, headers = headers)  
    
    try:
      httpStatusCode = response.status_code 
      if response == None:
          if verbose: print("Error: No output from service")
          if exitIfNoResponse: sys.exit()
          else: return False
      output = json.loads(response.text)
      if output['errorCode'] != None:
          if verbose: print(f"Error: {output['errorCode']} - {output['errorMessage']}")
          if exitIfNoResponse: sys.exit()
          else: return False
      if  httpStatusCode == 404:
          if verbose: print("Error: 404 Not Found")
          if exitIfNoResponse: sys.exit()
          else: return False
      elif httpStatusCode == 401: 
          if verbose: print("Error: 401 Unauthorized")
          if exitIfNoResponse: sys.exit()
          else: return False
      elif httpStatusCode == 400:
          if verbose: print("Error: ", httpStatusCode)
          if exitIfNoResponse: sys.exit()
          else: return False
    except Exception as e: 
          response.close()
          if verbose: print(f"Error: {e}")
          if exitIfNoResponse: sys.exit()
          else: return False
    response.close()
    
    return output['data']


""" Download a file from the specified URL. Will append to completed_list if provided. """
def download_file(url, completed_list = [], max_tries=5, verbose=True):
    sema.acquire()
    try:        
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        if verbose: print(f"Downloading {filename}...")
        if path != "" and path[-1] != "/":
            filename = "/" + filename
        open(path+filename, 'wb').write(response.content)
        if verbose: print(f"Downloaded {filename}.")
        completed_list.append(filename)
        sema.release()
    except Exception as e:
        sema.release()
        if verbose: print(f"Failed to download from {url}: {e}")
        if max_tries > 0:
            if verbose: print("Attempting to redownload...")
            threaded_download(url, threads, completed_list, max_tries - 1, verbose=verbose)
        else:
            if verbose: print("Max number of tries exceeded. Aborting...")
    
""" Download a file from the specified URL in a new thread. Will append to completed_list if provided. """    
def threaded_download(url, threads, completed_list=[], max_tries=5, verbose=True):
    thread = threading.Thread(target=download_file, args=(url, completed_list, max_tries, verbose))
    threads.append(thread)
    thread.start()
    

""" Extract either a .zip or a .tar archive. If provided, will extract to outfolder.
    If delete is set to True, it will delete the archive after extraction. """
def extract(file, outfolder=None, delete=False):
    filename, ext = os.path.splitext(file)
    if not outfolder:
        outfolder = filename
    if ext == ".zip":
        extract_zip(file, outfolder)
    elif ext == ".tar":
        extract_tar(file, outfolder)
    if delete:
        os.remove(file)
    return outfolder


def extract_zip(file, outfolder="."):
    with zipfile.ZipFile(file, 'r') as zip:
        zip.extractall(path=outfolder)


def extract_tar(file, outfolder="."):
    with tarfile.open(file, 'r') as tar:
        tar.extractall(path=outfolder)


def build_vrt(output=None, folder=".", bands=None, **kwargs):
    if not bands:
        # if no bands are specified, use all files in folder for the vrt
        bands = os.listdir(folder)
    band_filenames = get_matching_files(folder, bands)
    vrt_options = gdal.BuildVRTOptions(separate=True, **kwargs)
    vrt = gdal.BuildVRT(output, band_filenames, options=vrt_options)
    # free data so it writes to disk properly
    vrt = None
    return band_filenames


""" Given two tifs representing a red and NIR band respectively, calculated the
    NDVI and save it as a new tif.
    Assumes the red & NIR bands have the same projection, size, and geotransform.
    Behavior is undefined otherwise. """
def calc_ndvi(red_band, nir_band, folder=".", outname=None):
    red_band_list = get_matching_files(folder, [red_band])
    nir_band_list = get_matching_files(folder, [nir_band])
    if len(red_band_list) != 1 or len(nir_band_list) != 1:
        raise ValueError("Error: red band and NIR band should each match exactly one tif file")
    red_band_file = red_band_list[0]
    nir_band_file = nir_band_list[0]
    
    red_ds = gdal.Open(red_band_file)
    red = red_ds.GetRasterBand(1).ReadAsArray().astype(np.float64)
    nir_ds = gdal.Open(nir_band_file)
    nir = nir_ds.GetRasterBand(1).ReadAsArray().astype(np.float64)
    
    # TODO: this throws a warning, but the actual file looks fine. I'm guessing
    # it has to do with the fact that the tifs are rotated--maybe numpy is padding
    # them with zeroes to compensate, which causes some calculations to be 0/0.
    ndvi = np.divide(np.subtract(nir, red), np.add(nir, red))
    ndvi = np.where(ndvi == np.inf, np.nan, ndvi)
    # match location and general formatting of other bands so we place ndvi band
    # in the correct spot and name it similarly
    regex = re.compile(rf"""
                        (?P<path>.*[/\\])?                           # match any folders before band name
                        (?P<band_name>.*(?={red_band}(.tif|.TIF)))   # match general band name before suffix (ex. everything up to B5.tif)
                        """, re.VERBOSE)
    m = regex.match(red_band_file)
    ndvi_band_file = f"{m.group('path')}{m.group('band_name')}NDVI.TIF"
    
    # base projection, size, and geotransform off red band
    model_ds = gdal.Open(red_band_file)
    gt = model_ds.GetGeoTransform()
    proj = model_ds.GetProjection()
    xsize = red.shape[1]
    ysize = red.shape[0]
    
    driver = gdal.GetDriverByName("GTiff")
    driver.Register()
    outds = driver.Create(ndvi_band_file, 
                          xsize = xsize, 
                          ysize = ysize,
                          bands = 1,
                          eType = gdal.GDT_Float32)
    outds.SetGeoTransform(gt)
    outds.SetProjection(proj)
    outband = outds.GetRasterBand(1)
    outband.WriteArray(ndvi)
    outband.SetNoDataValue(np.nan) 
    outband.FlushCache()
    
    # free data so it writes to disk properly
    red_ds = red = nir_ds = nir = ndvi = model_ds = gt = proj = outds = outband = None
    return ndvi_band_file
    
    
""" Given a folder and a list of substrings, return all files that match one of the substrings.
    A downloaded Landsat-8 product, for example, will include several bands as well as metadata
    files, so we use this to only select the ones we care about, such as the RGB bands. """
def get_matching_files(folder=".", files=[]):
    filenames = []
    for file in files:
        match_pattern = f"**{os.path.sep}*{folder}*{os.path.sep}**{os.path.sep}*{file}*"
        matches = glob.glob(match_pattern, recursive=True)
        filenames.extend(matches)
    return filenames


""" Given the name of a file on local storage, upload it to an s3 bucket then delete the local copy. """
def upload_to_s3(filename, bucket, prefix, s3):
    try:
        key = s3_join(prefix, filename)
        print("Uploading " + filename + " to s3...")
        s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=key)
    except ClientError as e:
        print("Error while trying to upload " + filename + ": " +  e)
    os.remove(filename)
    
""" Intelligently join objects of elements with forward slashes.
    os.path.join will use backslashes on Windows machines, which must be corrected. """
def s3_join(*elements):
    if len(elements) > 0:
        return os.path.join(*elements).replace("\\", "/")
    return ""