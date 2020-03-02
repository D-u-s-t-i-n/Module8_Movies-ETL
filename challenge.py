# Import Dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import psycopg2
import time

# Wikipedia data
file_dir = 'Data' #'C:/Users/Username/DataBootcamp/'
file_wikiJSON = f'{file_dir}wikipedia.movies.json'
#f'{file_dir}filename'

# Kaggle
#kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
file_kaggleCSV = f'{file_dir}movies_metadata.csv'

# Ratings
#ratings = pd.read_csv(f'{file_dir}ratings.csv')
file_ratingsCSV = f'{file_dir}ratings.csv'

# First 5 records
#wiki_movies_raw[:5]


# Main function to check if file is ready
def ExecutePoll(filename):
    if ~path.exists(filename) | ~path.exists(file_kaggleCSV) | ~path.exists(file_ratingsCSV):
        print('begin')
    else:
        print('Movie file does not exist')


def CleanData(file_wiki, file_kaggle, file_ratings):
    with open(f'{file_wiki}', mode='r') as file: #with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
    


