import csv
from enum import Enum
import json
import argparse
import pandas as pd
from datetime import datetime

def format_history_data(data, run_time):
    eventList = []
    data = json.loads(data)
    for source, sourceEventList in data.items():
        for events in sourceEventList:
            try:
                eventSplit = events.split("-")
                title = ("-").join(events.split("-"[0:len(eventSplit)-1]))
                time = events.split("-")[-1].strip()
                time = datetime.strptime(time, '%m/%d/%Y %I:%M:%S %P')
            except(ValueError) as e:
                time = datetime.strptime(run_time,'%m/%d/%Y %I:%M:%S %P')
                title = events
            eventList.append((title, time, source))
    return eventList
                

def get_history_list(shop_history, run_time):
    shop_history_list = format_history_data(shop_history, run_time)
    
    title_set = set()
    formatted_shop_history_list = []
    sorted_shop_history_list = sorted(shop_history_list, key=lambda x: x[1], reverse=True)
    for title, time, source in sorted_shop_history_list:
        if(title.lower().strip() not in title_set and len(title) > 0):
            formatted_shop_history_list.append(title.lower() + "-" + str(time))
            title_set.add(title.lower().strip())
    return formatted_shop_history_list

def read_data(user_set_file: str, shop_history_file: str):
    muid_list = pd.read_csv(user_set_file, sep="\t", header=None)
    muid_list.colums = ["Muid"]
    
    raw_shop_history = pd.read_csv(shop_history_file, sep="\t")
    raw_shop_history.columns = ["Muid", "UserContext"]
    
    print("Muid list size", muid_list.shape)
    print("Raw shop history size", raw_shop_history.shape)

def write_data(output_file: str, user_se_shop_history: pd.DataFrame):
    print("out_put shape", user_se_shop_history.shape)
    user_set_shop_history.to_csv(output_file, sep="\t", index=False, quoting=csv.QUOTE_NONE)

def prepare_useractivity_data(input_shop_history, user_set_df, run_time):
    formatted_shop_history = input_shop_history.apply(lambda x: get_history_list(x["UserContext"], run_time), axis=1)
    activity_len = formatted_shop_history.apply(lambda x: len(x))
    input_shop_history["Formatted_shop_history"] = formatted_shop_history
    input_shop_history["Activity_len"] = activity_len
    
    user_set_shop_history = pd.merge(user_set_df, input_shop_history, on="Muid", how="inner")
    user_set_shop_history=user_set_shop_history[["Muid","Formated_shop_history", "Activity_len"]]
    user_set_shop_history.columns = ["UserId", "UserActivity", "UserActivityLen"]
    return user_set_shop_history

if __name__ == "__main__":
    # parse argument
    parser = argparse.ArgumentParser()
    parser.add_argument("--user_set_file", type=str, help="Path to the user set file")
    parser.add_argument("--shop_history_file", type=str, help="Path to the file with shop history")
    parser.add_argument("--run_time", type=str, help="Run time to be used for bing products' time stamp")
    parser.add_argument("--output_file", type=str, help="Path to the output file")
    
    args = parser.parse_args()
    user_set_file = args.user_set_file
    shop_history_file = args.shop_history_file
    run_time = args.run_time
    output_file = args.output_file
    
    user_set_df, input_shop_history = read_data(user_set_file, shop_history_file)
    user_set_shop_history = prepare_useractivity_data(input_shop_history, user_set_df, run_time)
    write_data(output_file, user_set_shop_history)
