# -*- coding: utf-8 -*-
"""
Created on Fri Nov 17 10:43:27 2023

@author: JLHE2
"""
# Python Libraries
import datetime
import struct
import zipfile

# Third Party Libraries
import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline
from scipy.signal import butter, sosfiltfilt


def get_hc_over_h(Am, h_max, area_coefficients):
    h = 0
    while berk_area_function(h, area_coefficients) < Am * 1e6:
        h += 1
    return h / h_max


def berk_area_function(h, area_coeffs):
    return (
        area_coeffs[0] * np.abs(h) ** 2
        + area_coeffs[1] * np.abs(h)
        + area_coeffs[2] * np.sqrt(np.abs(h))
        + area_coeffs[3] * np.abs(h) ** (0.25)
        + area_coeffs[4] * np.abs(h) ** (0.125)
    )


def bin_to_df(fname):  # Written by Chris Walker
    max_channel_size = int(1.25e6 * 30)  # Max allowable channel size
    # Open Zip File
    with zipfile.ZipFile(fname) as zp:
        binary_file = zp.namelist()[0]  # Find first binary file in zip
        zip_binary = zp.read(binary_file)  # Read binary file

        # Decode start tic as unsigned long long, convert to time
        start = datetime.timedelta(
            microseconds=struct.unpack("q", zip_binary[0:8])[0] / 10
        )
        # Decode stop tic as unsigned long long, convert to time
        stop = datetime.timedelta(
            microseconds=struct.unpack("q", zip_binary[8:16])[0] / 10
        )

        # calculate elapsed time, NOT REAL TIME OF TEST DUE TO DELAYS
        time = stop - start

        # Decode number of points as unsigned long long
        number_of_points = struct.unpack("q", zip_binary[16:24])[0]

        # Decode delta as double
        delta = struct.unpack("d", zip_binary[24:32])[0]
        print(f"Total Time (est): {time}")
        print(f"Points per Channel: {number_of_points}")
        print(f"Delta: {delta}")

        # Interpret all channels as 64 bit integers (long long)
        # skip first 32 bytes used for metadata
        buffstart = 32

        # Laser 1, in picometers
        buffend = buffstart + number_of_points * 8
        c1 = np.frombuffer(zip_binary[buffstart:buffend], dtype="int64")

        # # Laser 2, in picometers
        # buffstart = 32 + (max_channel_size * 8)
        # buffend = buffstart + number_of_points*8
        # c2 = np.frombuffer(
        #     zip_binary[buffstart:buffend],
        #     dtype='int64'
        # )

        # # Laser 3, in picometers
        # buffstart = 32 + (2 * max_channel_size * 8)
        # buffend = buffstart + number_of_points*8
        # c3 = np.frombuffer(
        #     zip_binary[buffstart:buffend],
        #     dtype='int64'
        # )

        # ADC 1, in Integer (min 0, max 2^16)
        buffstart = 32 + (3 * max_channel_size * 8)
        buffend = buffstart + number_of_points * 8
        c4 = np.frombuffer(zip_binary[buffstart:buffend], dtype="int64")

    # ADC 3, in Integer (min 0, max 2^16)
    buffstart = 32 + (4 * max_channel_size * 8)
    buffend = buffstart + number_of_points * 8
    c5 = np.frombuffer(zip_binary[buffstart:buffend], dtype="int64")

    # Create a Time channel based on data rate, assuming constant rate
    c6 = np.linspace(0, time.total_seconds(), number_of_points)

    df = pd.DataFrame(
        {
            "Laser1": c1,
            # 'Laser2': c2,
            # 'Laser3': c3,
            "ADC1": c4,
            "ADC3": c5,
            "Time": c6,
        }
    )
    del zip_binary
    return df, delta


# Export impact laser data should export the derivatives load, piezo load, depth, and time data from impact data
# into a Pandas data frame. Further analysis on load-depth curve can be performed on the
# output data frame. The data frames associated with multiple tests can also be saved as
# separate sheets. We should include the measured contact areas from the Keyence in the
# results excel sheet, assuming that the user has measured contact areas.


def calculate_H(load, displacement, tip_area_coeffs, area_max_depth, hc_over_h=1):
    if area_max_depth != 0:
        hc_over_h = get_hc_over_h(area_max_depth, max(displacement), tip_area_coeffs)
        print(hc_over_h)
    else:
        hc_over_h = 1
    depth = displacement * hc_over_h
    A = berk_area_function(depth, area_coeffs=tip_area_coeffs)
    H = load / A * 1e6
    return H, A, hc_over_h


def downsample_list(data, hop):
    return data[::hop]


def export_CSR_laser_data(
    filename, actuator_params, Kf=8e6, downsample=True, downsample_hop=20
):
    df, delta = bin_to_df(filename)

    time = df.Time
    displacement = df.Laser1 / 1000
    force = df.ADC3 / actuator_params[3]

    # Passes filter displacement data set through a low-pass filter.
    # This is used to calculate the strain rate.
    sos = butter(N=6, Wn=500, btype="low", analog=False, fs=1 / delta, output="sos")
    force = sosfiltfilt(sos, force)

    # Shifts data so that h, force = 0 at the beginning.
    F0 = force[0]
    h0 = displacement[0]
    t0 = time[0]
    force -= F0
    displacement -= h0
    time -= t0

    # Passes filter displacement data set through a low-pass filter.
    # This is used to calculate the strain rate.
    sos = butter(N=6, Wn=100, btype="low", analog=False, fs=1 / delta, output="sos")
    filtered_displacement = sosfiltfilt(sos, displacement)

    # Splines data to obtain derivatives.
    h_spl = CubicSpline(time, filtered_displacement)
    dhdt_spl = h_spl.derivative(1)
    v = dhdt_spl(time) * 1e-9

    del filtered_displacement

    # Obtains load from derivatives method in units of mN.
    load = (force * 1e-3 - displacement * 1e-9 * actuator_params[0]) * 1e3

    displacement -= (load / Kf) * 1e6

    SR = v * 1e9 / displacement

    if downsample:
        # Downsamples the lists
        load = downsample_list(load, downsample_hop)
        displacement = downsample_list(displacement, downsample_hop)
        SR = downsample_list(SR, downsample_hop)
        time = downsample_list(time, downsample_hop)

    return time.to_numpy(), load.to_numpy(), displacement.to_numpy(), SR.to_numpy()


def get_Am_HTMDEC(filename, sample_id, indent_num):
    print(f"Looking for {sample_id}_CSR_I{indent_num:02d} in {filename}")
    try:
        df = pd.read_excel(filename, sheet_name="Volume & area")[3:]
    except FileNotFoundError:
        print(
            "Could not find area measurement file. Returning 0 and assuming hc/h = 1 from here-on."
        )
        return 0
    return (
        df.loc[
            df["File name"] == f"{sample_id}_CSR_I{indent_num:02d}", "Volume & area.1"
        ]
        .astype(float)
        .values[0]
    )
