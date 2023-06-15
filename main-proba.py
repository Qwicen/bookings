import os
import yaml
import numpy as np
import pandas as pd
from datetime import date
from db_utils import connect_db
from collections import defaultdict


if __name__ == "__main__":
    dirname = os.path.abspath(os.path.dirname(__file__))
    db_connection = connect_db(os.path.join(dirname, './configs/db_config.yaml'))
    cursor = db_connection.cursor()
    objects = yaml.safe_load(open(os.path.join(dirname, "./configs/objects.yaml"), 'r'))
    room_id = {'Апартаменты': 0, 'Коттедж': 1, 'Стандарт': 2, 'Студия': 3}
    today_date = date.today().isoformat()

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
                                    month(date_in)={month};
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
