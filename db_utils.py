import yaml
import numpy as np
import pandas as pd
import mysql.connector as connection
from datetime import date, timedelta
from booking_matrix import binarize

def connect_db(config_path):
    config = yaml.safe_load(open(config_path, 'r'))    
    try:
        db_connection = connection.connect(**config)
    except Exception as e:
        print(str(e))
    return db_connection

def get_bookings_from_db(object_id, room_type, date_start, date_end, pred_horizont, config_path='./configs/db_config.yaml'):
    db_connection = connect_db(config_path)
    cutoff_date = date.fromisoformat(date_end) + timedelta(days=pred_horizont)
    cutoff_date = cutoff_date.isoformat()
    query = f"""
        SELECT 
            id_object, room_type_agg, date_book, date_in, nights 
        FROM predict_soft.data_booking
        WHERE 
            id_object={object_id} AND 
            room_type_agg=\"{room_type}\" AND
            date_in>=\"{date_start}\" AND
            date_in<=\"{cutoff_date}\"
    """
    df = pd.read_sql(query, db_connection)
    db_connection.close()
    return df

def push_predictions_to_db(obj, object_id, room, today_date, pred, config_path="./configs/db_config.yaml"):
    db_connection = connect_db(config_path)
    cursor = db_connection.cursor()
    query = ("INSERT INTO demand_cal "
             "(id_object, room_type_agg, date_calc, date_in, value, rate) "
             "VALUES (%s, %s, %s, %s, %s, %s)")
    demand = binarize(pred, obj, room)
    for day_idx in range(pred.shape[0]):
        date_in = date.fromisoformat(today_date) + timedelta(days=day_idx)
        value = round(pred[day_idx], 2)
        cursor.execute(query, (object_id, room, today_date, date_in.isoformat(), value, demand[day_idx]))
    db_connection.commit()
    cursor.close()
    db_connection.close()
    
def push_predictions_to_db_in_matrix_form(obj, room, today_date, pred, config_path='./configs/db_config.yaml'):
    db_connection = connect_db(config_path)
    cursor = db_connection.cursor()
    query = ("INSERT INTO matrix_prediction "
             "(id_object, room_type_agg, date_calc, date_book, date_in, value) "
             "VALUES (%s, %s, %s, %s, %s, %s)")
    for day_idx in range(pred.shape[0]):
        for depth_idx in range(day_idx + 1):
            date_in = date.fromisoformat(today_date) + timedelta(days=day_idx)
            date_book = date_in - timedelta(days=depth_idx)
            value = round(pred[day_idx, depth_idx], 2)
            cursor.execute(query, (obj, room, today_date, date_book.isoformat(), date_in.isoformat(), value))
    db_connection.commit()
    cursor.close()
    db_connection.close()
