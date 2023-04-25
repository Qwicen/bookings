import yaml
import argparse
from datetime import date, timedelta
from db_utils import get_bookings_from_db
from db_utils import push_predictions_to_db
from booking_matrix import build_booking_matrix
from booking_matrix import build_pred_matrix

import os
import time
from plot_utils import plot_pred
from metrics import MSE, MAPE
from torch.utils.tensorboard import SummaryWriter


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--horizont", type=int, default=30)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--n_days", type=int)
    group.add_argument("--date_start", type=str)
    #args = parser.parse_args(["--date_start", "2021-06-01"])
    args = parser.parse_args(["--n_days", "365"])

    today_date = "2022-06-17" #date.today().isoformat()
    if args.n_days is not None:
        start_date = (date.fromisoformat(today_date) - timedelta(days=args.n_days)).isoformat()
    else:
        start_date = args.date_start
    
    print(today_date)
    print(start_date)
    print(args.horizont)

    experiment_name = 'default'
    experiment_name = '{}_{}.{:0>2d}.{:0>2d}_{:0>2d}:{:0>2d}'.format(experiment_name, *time.gmtime()[:5])
    experiment_path = os.path.join('logs/', experiment_name)
    writer = SummaryWriter(experiment_path)
    objects = yaml.safe_load(open("./configs/objects.yaml", 'r'))
    for obj in objects:
        for room in objects[obj]:
            df = get_bookings_from_db(objects[obj]["object_id"], room, start_date, today_date, args.horizont)
            if len(df) == 0: continue
            X, C, idx_to_date, date_to_idx = build_booking_matrix(df, start_date, today_date, args.horizont)
            pred, C_pred = build_pred_matrix(C, date_to_idx, today_date, args.horizont)
            daily_pred = C_pred[:,0]
            #push_predictions_to_db(obj, objects[obj]["object_id"], room, today_date, daily_pred)
            #plot_pred(C, C_pred, args.horizont, today_date, obj, room)

            for h in [7, 14, 30]:
                mse = MSE(C[-args.horizont:,0], daily_pred, horizont=h)
                mape = MAPE(C[-args.horizont:,0], daily_pred, horizont=h)
                writer.add_scalar(f'mse/{obj}/{room}', mse, h)
                writer.add_scalar(f'mape/{obj}/{room}', mape, h)
            