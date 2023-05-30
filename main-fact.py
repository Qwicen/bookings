import yaml
import argparse
from datetime import date, timedelta
from db_utils import get_bookings_from_db
from db_utils import push_predictions_to_db
from booking_matrix import build_booking_matrix


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--horizont", type=int, default=124)
    parser.add_argument("--n_days", type=int, default=1)
    args = parser.parse_args()

    today_date = date.today().isoformat()
    if args.n_days is not None:
        start_date = (date.fromisoformat(today_date) - timedelta(days=args.n_days)).isoformat()
    else:
        start_date = args.date_start
    
    objects = yaml.safe_load(open("./configs/objects.yaml", 'r'))
    for obj in objects:
        for room in objects[obj]["room_types"]:
            df = get_bookings_from_db(objects[obj]["object_id"], room, start_date, today_date, args.horizont)
            if len(df) == 0: continue
            X, C, idx_to_date, date_to_idx = build_booking_matrix(df, start_date, today_date, args.horizont)
            daily_fact = C[:,0]
            push_predictions_to_db(obj, objects[obj]["object_id"], room, today_date, daily_fact, table='demand_fact')