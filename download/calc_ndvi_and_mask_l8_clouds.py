import os
import re

import numpy as np
from osgeo import gdal

from util import get_matching_files


def calc_ndvi_and_mask_l8_clouds(red_band, nir_band, qa_band, folder="."):
    red_band_list = get_matching_files(folder, [red_band])
    nir_band_list = get_matching_files(folder, [nir_band])
    qa_band_list = get_matching_files(folder, [qa_band])
    if len(red_band_list) != 1 or len(nir_band_list) != 1 or len(qa_band_list) != 1:
        raise ValueError("Error: provided band names should each match exactly one tif file")
    red_band_file = red_band_list[0]
    nir_band_file = nir_band_list[0]
    qa_band_file = qa_band_list[0]
    
    # open red, nir, and qa tif files
    red_ds = gdal.Open(red_band_file)
    red = red_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    nir_ds = gdal.Open(nir_band_file)
    nir = nir_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    qa_ds = gdal.Open(qa_band_file)
    qa = qa_ds.GetRasterBand(1).ReadAsArray()
    
    # calculate NDVI
    # TODO: this throws a warning, but the actual file looks fine. I'm guessing
    # it has to do with the fact that the tifs are rotated--maybe numpy is padding
    # them with zeroes to compensate, which causes some calculations to be 0/0.
    ndvi = np.divide(np.subtract(nir, red), np.add(nir, red))
    ndvi = np.where(ndvi == np.inf, np.nan, ndvi)
    
    # calculate cloud mask
    # bits 3 and 4 are cloud and cloud shadow, respectively
    cloud_bit = 1 << 3
    cloud_shadow_bit = 1 << 4
    bit_mask = cloud_bit | cloud_shadow_bit
    cloud_mask = ((np.bitwise_and(qa, bit_mask) != 0) * 1).astype(np.int16)
    
    # update mask of target_band
    ndvi_masked = np.where(cloud_mask, np.nan, ndvi)
    
    # get gt, projection, and size from red band
    gt = red_ds.GetGeoTransform()
    proj = red_ds.GetProjection()
    xsize = red_ds.RasterXSize
    ysize = red_ds.RasterYSize
    
    # match location and general formatting of other bands so we place ndvi band
    # in the correct spot and name it similarly
    regex = re.compile(rf"""
                        (?P<path>.*[/\\])?                           # match any folders before band name
                        (?P<band_name>.*(?={red_band}(.tif|.TIF)))   # match general band name before suffix (ex. everything up to B5.tif)
                        """, re.VERBOSE)
    m = regex.match(red_band_file)
    ndvi_masked_file = f"{m.group('path')}{m.group('band_name')}NDVI_MASKED.TIF"
    
    # write mask to new file
    driver = gdal.GetDriverByName("GTiff")
    driver.Register()
    out_ds = driver.Create(ndvi_masked_file,
                          xsize = xsize,
                          ysize = ysize,
                          bands = 1,
                          eType = gdal.GDT_Float32)
    out_ds.SetGeoTransform(gt)
    out_ds.SetProjection(proj)
    outband = out_ds.GetRasterBand(1)
    outband.WriteArray(ndvi_masked)
    outband.SetNoDataValue(np.nan)
    outband.FlushCache
    
    # free data so it saves to disk properly
    red_ds = nir_ds = qa_ds = out_ds = outband = gt = proj = xsize = ysize = driver = None
    red = nir = qa = ndvi_masked = None
    
    return ndvi_masked_file
    
def main():
    # TODO: make this a lambda function.
    # right now this is more of a proof of concept
    folder = "LC08_L2SP_008056_20210715_20210721_02_T1"
    calc_ndvi_and_mask_l8_clouds("B4", "B5", "QA_PIXEL", folder)


if __name__ == "__main__":
    main()