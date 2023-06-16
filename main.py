import os
import yaml
import argparse
from datetime import date, timedelta
from db_utils import get_bookings_from_db
from db_utils import push_predictions_to_db
from booking_matrix import build_booking_matrix
from booking_matrix import build_pred_matrix


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--horizont", type=int, default=30)
    parser.add_argument("--n_days", type=int, default=365)
    args = parser.parse_args()
    dirname = os.path.abspath(os.path.dirname(__file__))
    config_path = os.path.join(dirname, './configs/db_config.yaml')
    objects_path = os.path.join(dirname, "./configs/objects.yaml")
    today_date = date.today().isoformat()
    start_date = (date.fromisoformat(today_date) - timedelta(days=args.n_days)).isoformat()
    
    objects = yaml.safe_load(open(objects_path, 'r'))
    room_id = {'Стандарт': 0, 'Апартаменты': 1, 'Коттедж': 2, 'Студия': 3}
    for obj in objects:
        for room in objects[obj]["room_types"]:
            df = get_bookings_from_db(objects[obj]["object_id"], room, start_date, today_date, args.horizont,
                                      config_path=config_path)
            if len(df) == 0: continue
            X, C, idx_to_date, date_to_idx = build_booking_matrix(df, start_date, today_date, args.horizont)
            pred, C_pred = build_pred_matrix(C, date_to_idx, today_date, args.horizont)
            daily_pred = C_pred[:,0]
            push_predictions_to_db(obj, objects[obj]["object_id"], room, room_id[room], today_date, daily_pred,
                                   config_path=config_path, objects_path=objects_path)
