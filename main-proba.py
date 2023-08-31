import os
import yaml
import numpy as np
import pandas as pd
from datetime import date, timedelta
from db_utils import connect_db
from collections import defaultdict


if __name__ == "__main__":
    dirname = os.path.abspath(os.path.dirname(__file__))
    db_connection = connect_db(os.path.join(dirname, './configs/db_config.yaml'))
    cursor = db_connection.cursor()
    objects = yaml.safe_load(open(os.path.join(dirname, "./configs/objects.yaml"), 'r'))
    room_id = {'Стандарт': 0, 'Апартаменты': 1, 'Коттедж': 2, 'Студия': 3}
    today_datetime = date.today()
    f_date = today_datetime - timedelta(days=(today_datetime.day - 1))
    s_date = date(f_date.year - 1, f_date.month, f_date.day)
    f_date = f_date.isoformat()
    s_date = s_date.isoformat()
    today_date = today_datetime.isoformat()

    insert_query = ("INSERT INTO probability "
        "(id_object, room_type_agg, index_name, month, weekday, value) "
        "VALUES (%s, %s, %s, %s, %s, %s)")
    
    for obj in objects:
        for room_type in objects[obj]["room_types"]:
            probas = defaultdict(lambda: defaultdict(float))
            for month in range(1,13):
                for weekday in range(7):
                    query = f"""
                                SELECT 
                                    id_object, room_type_agg, date_in, status_book 
                                FROM predict_soft.data_booking
                                WHERE 
                                    id_object={objects[obj]["object_id"]} AND 
                                    room_type_agg=\"{room_type}\" AND
                                    weekday(date_in)={weekday} AND
                                    month(date_in)={month} AND
                                    date_in>=\"{s_date}\" AND
                                    date_in<\"{f_date}\";
                            """
                    df = pd.read_sql(query, db_connection)
                    if len(df) == 0:
                        rate = 0
                    else:
                        rate = np.sum(df.status_book == 'Активный') / len(df)
                    value = np.round(rate, 2).astype(float)
                    probas[month][weekday] = value
                    cursor.execute(insert_query, (objects[obj]["object_id"], room_id[room_type], today_date, month, weekday, value))
            db_connection.commit()
    cursor.close()
    db_connection.close()
