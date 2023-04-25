import yaml
import numpy as np
import pandas as pd
from datetime import date, timedelta


def build_booking_matrix(df, date_start, date_end, pred_horizont):
    date_start = date.fromisoformat(date_start)
    cutoff_date = date.fromisoformat(date_end) + timedelta(days=pred_horizont-1)
    df.date_book = df.date_book.apply(lambda x: date.fromisoformat(x))
    df.date_in = df.date_in.apply(lambda x: date.fromisoformat(x))
    df["book_delta"] = df.date_in - df.date_book
    df["total_delta"] = df.book_delta + df.nights.apply(lambda x: timedelta(x))
    days_count = int((cutoff_date - date_start).days) + 1
    max_delta = int(df.total_delta.max().days)
    booking_X = np.zeros((days_count, max_delta))
    
    for day_idx in range(days_count):
        current_date = date_start + timedelta(days=day_idx)
        for booking in df[df.date_in == current_date].iterrows():
            for night in range(booking[1].nights):
                current_night = current_date + timedelta(night)
                if current_night <= cutoff_date:
                    booking_X[day_idx + night, booking[1].book_delta.days + night] += 1
        
    booking_C = np.cumsum(booking_X[:,::-1], axis=1)[:,::-1]
    idx_to_date = {day_idx : date_start + timedelta(days=day_idx) for day_idx in range(days_count)}
    date_to_idx = {(date_start + timedelta(days=day_idx)).isoformat() : day_idx for day_idx in range(days_count)}
    return booking_X, booking_C, idx_to_date, date_to_idx

def build_pred_matrix(booking_C, date_to_idx, date_end, pred_horizont):
    idx_end = date_to_idx[date_end]
    relations = np.zeros(pred_horizont)
    predictions = np.copy(booking_C[-pred_horizont-1:, :pred_horizont+1])
    for day_idx in range(pred_horizont):
        numer = np.sum(booking_C[:idx_end + day_idx, day_idx])
        denom = np.sum(booking_C[:idx_end + day_idx, day_idx + 1])
        relations[day_idx] = numer / denom
    
    for day_idx in range(1, pred_horizont + 1):
        for depth in range(1 , day_idx + 1):
            predictions[day_idx, day_idx - depth] = predictions[day_idx, day_idx - depth + 1] *\
                                                    relations[day_idx - depth]
    noncum_pred = -np.diff(predictions)[1:]
    return noncum_pred, predictions[1:,:-1]

def binarize(bookings, object, room_type, config_path="./configs/objects.yaml"):
    bins = yaml.safe_load(open(config_path, 'r'))
    indices = np.digitize(bookings, bins[object][room_type], right=True)
    demand = np.vectorize({0: "Low", 1: "Neutral", 2 : "High"}.get)(indices)
    return demand
