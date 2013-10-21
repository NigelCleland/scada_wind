import pandas as pd
import numpy as np
import datetime
import glob
import os
import matplotlib.pyplot as plt

def load_series(fName, column):
    """ Load a single series from the Filename with the index set by Epoc
    Seconds
    """

    df = pd.read_csv(fName)
    df.index = df["Epoc Seconds"]
    return df[column].copy()


def load_folder(directory, farm="West Wind", column=None):

    all_files = []
    for fdir, subdir, files in os.walk(directory):
        if farm.replace(' ', '') in fdir:
            for f in files:
                all_files.append(os.path.join(fdir, f))

    return pd.concat([load_series(f, column) for f in all_files], axis=1)


def stream_plot(df):

    fig, axes = plt.subplots(1,1, figsize=(16,9))

    for q in [98, 95, 90, 75, 50, 25, 10, 5, 2]:
        label = "%s Percentile" % q
        l = df.apply(np.percentile, q=q, axis=1).plot(ax=axes, label=label)

    axes.legend()

    return fig, axes

def process_and_plot(directory, column="Cumulative Deviation"):

    wind_farms = ("West Wind", "Tararua", "Te Apiti", "All Tararua",
                  "Tararua WC", "White Hill", "Mahinerangi", "Te Uku",
                  "All", "North Island", "South Island")

    for farm in wind_farms:

        df = load_folder(directory, farm=farm, column=column)

        fig, axes = stream_plot(df)
        axes.set_title(farm)
        axes.set_xlabel("Time Since Epoc [s]", fontsize=16)
        axes.set_ylabel(column)
        axes.set_xlim(0, 300)
        savename = " ".join([farm, column])

        fig.savefig(savename, dpi=100)
        plt.close()
