"""
This script is used to aprse the raw model reponses from the UHRS task and extract the judgetment from them. 
"""
import argparse
import csv
import pandas as pd 

parser = argparse.ArgumentParser()
parser.add_argument("--input_file_txt", type = str,default = "input_file.txt")
parser.add_argument("--prompt_type", typ = str, default = "Simple")
parser.add_argument("--source", type = str, default="Cluster")
parser.add_argument("--date", type = str, help = "Date", default = pd.to_datetime("now").strftime("%Y-%m-%d"))
parser.add_argument("--addl_columns", type=str, default = "None")
parser.add_argument("--parsed_output_tsv", type = str, default = "parsed_output.tsv")
args = parser.parse_args()