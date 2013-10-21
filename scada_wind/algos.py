"""
Processing stages to work with the four second wind data
"""

import pandas as pd
import numpy as np
import os
import glob
import datetime
from pandas.tseries.offsets import Minute

def load_month(filename):
    """ Load a months worth of data and handle indices """

    df = pd.read_csv(filename, index_col=0)
    df.index = convert_datetime(df["Timestamp"])
    return df

def convert_datetime(series):
    time_format = "%d-%b-%y %H:%M:%S"
    return pd.to_datetime(series, format=time_format)

def wind_site_selector(columns, site):

    wind_farms = {"West Wind": ["WWD"],
                 "Tararua": ["TWF"],
                 "All Tararua": ["TWF", "TAP", "TWC"],
                 "Te Apiti": ["TAP"],
                 "Tararua WC": ["TWC"],
                 "White Hill": ["WHL"],
                 "Mahinerangi": ["MAH"],
                 "Te Uku": ["TUK"],
                 "All": ["GENERAT"],
                 "North Island": ["TWF", "TAP", "TWC", "WWD", "TRH", "TUK"],
                 "South Island": ["WHL", "MAH"]}

    return [x for x in columns if any([y in x for y in wind_farms[site]])]

def aggregate_farms(df, farm):
    cols = wind_site_selector(df.columns, farm)
    return df[cols].sum(axis=1)

def five_min_resample(series):
    return series.resample("5min", how="mean")

def five_min_delta(series):
    cutter = lambda x: x[-1] - x[0]
    return pd.rolling_apply(series, 2, func=cutter)

def five_min_cutoff(series, cutoff_point=0.4):
    cutoff_points = series[series <= -cutoff_point]
    cutoff_points.sort()
    return cutoff_points

def offsetter(x, minute=10):
    return (x - Minute(minute), x + Minute(minute))

def slicer(series, stamp):
    begin, end = offsetter(stamp)
    return series[begin:end]

def all_slicer(series, stamps):
    return [slicer(series, stamp) for stamp in stamps]

def process_series(series):
    five_minute = five_min_resample(series)
    deviation = five_min_delta(five_minute)
    deviation.sort()
    stamps = deviation.head().index
    return all_slicer(series, stamps)

def create_directory(filename):
    cwd = os.getcwd()
    new_name = os.path.join(cwd, os.path.splitext(filename)[0])
    if not os.path.exists(new_name):
        os.mkdir(new_name)

    return new_name

def create_farm_directory(dir, farm):
    new_name = os.path.join(dir, farm.replace(' ', ''))
    if not os.path.exists(new_name):
        os.mkdir(new_name)
    return new_name

def threshold_values(farm):
    """ Define a minimum threshold value for each wind farm to be passed
    to the cutoff values instead of choosing it relatively
    """

    thresholds = {"West Wind": 20,
                  "Tararua": 20,
                  "All Tararua": 30,
                  "Te Apiti": 20,
                  "Tararua WC": 20,
                  "White Hill": 20,
                  "Mahinerangi": 20,
                  "Te Uku": 30,
                  "All": 40,
                  "North Island": 40,
                  "South Island": 17}

    return thresholds[farm]

def process_save(df, farm_dir, farm):

    try:
        wind_series = aggregate_farms(df, farm)
        sliced_series = process_series(wind_series)

        for i, possible in enumerate(sliced_series):

            opt = "%s_Option_%s.csv" % (farm.replace(' ', '_'), i)
            savename = os.path.join(farm_dir, opt)
            possible.name = "Scada Output"
            possible.index.name = "Timestmap"
            possible.to_csv(savename, index=True, header=True)
    except:
        print "There was an Error processing", farm


def process_month(filename):

    df = load_month(filename)
    directory = create_directory(filename)

    wind_farms = ("West Wind", "Tararua", "Te Apiti", "All Tararua",
                  "Tararua WC", "White Hill", "Mahinerangi", "Te Uku",
                  "All", "North Island", "South Island")

    for farm in wind_farms:
        farm_dir = create_farm_directory(directory, farm)
        process_save(df, farm_dir, farm)

def run_directory(directory):

    files = glob.glob(os.path.join(directory, "*.csv"))
    for f in files:
        print f, "Begun"
        try:
            process_month(f)
            print f, "Complete"
        except:
            print f, "Unable to complete, investiate further"












