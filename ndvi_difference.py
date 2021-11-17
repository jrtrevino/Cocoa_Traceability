import sys
import argparse

import boto3
import pandas as pd
from pandas.tseries.offsets import DateOffset
from osgeo import gdal

""" Given a row in a DataFrame representing a granule, return the full filename
    of that granule in the s3 filesystem so GDAL can access it. """
def get_granule_filename(granule):
    return f"/vsis3/{granule['bucket']}/{granule['key']}"

def main():
    parser = argparse.ArgumentParser(
        description="Perform NDVI differencing on L8 scenes between two dates.")
    
    parser.add_argument("start", type=str,
                        help="first date (format: yyyy-mm-dd)")
    parser.add_argument("end", type=str,
                        help="second date (format: yyyy-mm-dd, should be later than start)")
    parser.add_argument("-years", "--y", dest="years", type=int, default=0,
                        help="how many years to look back to fill in missing data")
    parser.add_argument("-months", "--m", dest="months", type=int, default=0,
                        help="how many months to look back to fill in missing data")
    parser.add_argument("-days", "--d", dest="days", type=int, default=0,
                        help="how many days to look back to fill in missing data")
    args = parser.parse_args()
    
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)
    delta = DateOffset(years=args.years, months=args.months, days=args.days)
    
    start_d = start - delta
    end_d = end - delta
    
    # load database of granules
    # TODO: turn this into a proper database?
    # using a csv file as a temp measure
    granules = pd.read_csv("l8-granules.csv")
    granules['date'] = pd.to_datetime(granules['date'])
    granules = granules.sort_values(by="date", ascending=False)
    
    # search for granules between start_d and start
    start_df = granules[(granules['date'] >= start_d) &
                              (granules['date'] <= start)]
    start_granules = start_df.apply(get_granule_filename, axis=1).tolist()
    
    end_df = granules[(granules['date'] >= end_d) &
                              (granules['date'] <= end)]
    end_granules = end_df.apply(get_granule_filename, axis=1).tolist()
    
    # build vrts
    start_vrt = gdal.BuildVRT("start.vrt", start_granules)
    end_vrt = gdal.BuildVRT("end.vrt", end_granules)
    # build a combined vrt for calculation purposes
    combined = gdal.BuildVRT("combined.vrt", ["start.vrt", "end.vrt"], separate=True)
    
    # flush cache so GDAL writes to disk
    start_vrt = end_vrt = combined = None

if __name__ == "__main__":
    main()