import pandas as pd
import numpy as np
import datetime
import glob
import os

def load_option(filename):
    """ Loads as a Series and parses dates automatically """
    df = pd.read_csv(filename, index_col=0, parse_dates=True, dayfirst=True)
    return df["Scada Output"]


def instantaneous_deriv(array):
    """ Return average time weighted deltas from an array """

    delta = array[0] - array[1:]
    time_weighted_delta = delta / np.arange(4, len(array)*4, 4)
    return time_weighted_delta.mean()


def get_max_stamp(series):
    """ Get the time stamp at the epoc point """
    return series.index[series.argmax()]


def determine_epoc(series, window=75, func=instantaneous_deriv):
    """ Determine the Epoc Location from a Series,

    Takes an optional window size and deriviative function in order to
    calculate. Default window size is set to 300 seconds

    Will return the time stamp of the maximum deriviative point

    """

    # Apply the deriv function over the window
    changes = pd.rolling_apply(series, window=window+1, func=func)
    changes = changes.dropna()

    # Shift the index back to the original location.
    changes.index = series.index[:-window]

    return get_max_stamp(changes)


def from_epoc(series, epoc):
    """ Performs a couple time manipulations in order to determine
    relative timings
    """
    df = pd.DataFrame(series)
    df.index.name = "Timestamp"
    df["Timestamp"] = df.index
    df["Epoc Time"] = df["Timestamp"] - epoc
    df["Epoc Seconds"] = np.array((df["Timestamp"] - epoc).values / 1000000000, dtype=np.int64)
    return df

def cumulative_deviation(df, epoc):

    df["Cumulative Deviation"] = df["Scada Output"] - df["Scada Output"].ix[epoc]
    return df

def instantaneous_deviation(df, epoc):
    cutter = lambda x: x[0] - x[1]
    df["Instantaneous Deviation"] = pd.rolling_apply(df["Scada Output"],
                                                     2, cutter)
    return df

def average_deviation(df, epoc):
    df["Average Deviation"] = (df["Scada Output"] - df["Scada Output"].ix[epoc]) / df["Epoc Seconds"]

    return df

def percent_epoc_max(df, epoc):
    df["Percentage of Epoc"] = df["Scada Output"] / df["Scada Output"].ix[epoc]
    return df

def percent_capacity(df, capacity):
    df["Percentage of Capacity"] = df["Scada Output"] / capacity
    return df

def cumulative_delta_capacity(df, epoc):
    df["Delta Capacity"] = df["Percentage of Capacity"] - df["Percentage of Capacity"].ix[epoc]

    return df

def farm_capacity(farm):

    capacities = {"West Wind": 135,
                  "Tararua": 66,
                  "All Tararua": 250,
                  "Te Apiti": 92,
                  "Tararua WC": 90,
                  "White Hill": 57,
                  "Mahinerangi": 36,
                  "Te Uku": 64,
                  "All": 590,
                  "North Island": 500,
                  "South Island": 95}

    return capacities[farm]

def get_farm_name(fName):

    base = os.path.basename(fName)
    rmext = os.path.splitext(base)[0]
    rmopt = rmext.split('_')[:-2]
    return ' '.join(rmopt)

def process_option(fName):

    series = load_option(fName)
    stamp = determine_epoc(series)

    farm_name = get_farm_name(fName)
    capacity = farm_capacity(farm_name)

    df = from_epoc(series, stamp)
    df = cumulative_deviation(df, stamp)
    df = instantaneous_deviation(df, stamp)
    df = average_deviation(df, stamp)
    df = percent_epoc_max(df, stamp)
    df = percent_capacity(df, capacity)
    df = cumulative_delta_capacity(df, stamp)

    return df

def recursive_method(masterdirectory):

    nd_name = os.path.join(masterdirectory, "metric_data")
    if not os.path.exists(nd_name):
        os.mkdir(nd_name)

    full_dir = os.path.join(masterdirectory, "option_data")

    for directory, subdir, files in os.walk(full_dir):
        if files:
            new_dir = directory.replace('option_data', 'metric_data')
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

            for f in files:
                abs_path = os.path.join(directory, f)
                save_path = abs_path.replace('option_data', 'metric_data')
                df = process_option(abs_path)
                df.to_csv(save_path, header=True, index=True)
