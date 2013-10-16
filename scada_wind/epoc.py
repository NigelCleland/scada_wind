import pandas as pd
import numpy as np
import datetime
import glob
import os

def load_option(filename):
    df = pd.read_csv(filename, index_col=0, parse_dates=True, dayfirst=True)
    return df["Scada Output"]

def deviation(series, x):
    return pd.rolling_apply(series, x+1, func=lambda x: x[0] - x[-1])


def average_deriv(array):
    head, tail = array[0], array[1:]
    changes = (head - tail)
    return changes.mean()

def per_sec_deriv(array):
    head, tail = array[0], array[1:]
    changes = (head - tail)
    return changes.mean() / ((len(tail)+1) * 4)

def get_stamp(series):
    return series.index[series.argmax()]

def delta_time(series, begin_stamp, seconds):
    end_stamp = begin_stamp + datetime.timedelta(seconds=seconds)
    return series[begin_stamp] - series[end_stamp]

def find_epoc(series, window_size=75):
    changes = pd.rolling_apply(series, window_size+1, func=average_deriv)
    changes = changes.dropna()
    changes.index = series.index[:-window_size]
    return changes

def abs_metric(series, epoc, max_win=300):
    periods=np.arange(0., max_win+4., 4.)
    values = [delta_time(series, epoc, period) for period in periods]

    return pd.Series(values, index=periods)

def delta_metric(series, epoc, max_win=300):

    short_series = series[epoc:epoc+datetime.timedelta(seconds=max_win)].copy()
    delta = pd.rolling_apply(short_series, 6, func=per_sec_deriv)

    return pd.Series(delta.values, index=np.arange(0., max_win+4., 4.))

def act_value(series, epoc, max_win=300):

    short_series = series[epoc:epoc+datetime.timedelta(seconds=max_win)].copy()
    return pd.Series(short_series.values, index=np.arange(0., max_win+4., 4.))

def metric_df(series, max_win=300):

    epoc = get_stamp(find_epoc(series))

    absolute = abs_metric(series, epoc, max_win=max_win)
    absolute.name = "Absolute Changes [MW]"
    delta = delta_metric(series, epoc, max_win=max_win)
    delta = delta.fillna(0)
    delta.name = "Instantaneous Changes [MW]"
    actuals = act_value(series, epoc, max_win=max_win)
    actuals.name = "Actual Output [MW]"

    return pd.concat([absolute, delta, actuals], axis=1)

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
                series = load_option(abs_path)
                df = metric_df(series)
                df.index.name = "Seconds since epoc"
                df.to_csv(save_path, header=True, index=True)











