# Import Dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import psycopg2
#from psycopg2 import Error
import time
import os
import shutil

# Local variables to handle folders (incoming and finished data)
timestr = time.strftime("%Y%m%d-%H%M%S")
file_dir = 'Data/' #'C:/Users/Username/DataBootcamp/'
finished_dir = 'Done/'
# Global variable to ensure only successful cleaned + uploaded data is moved to the finished folder
bool_success = False

# Wikipedia data
file_wikiJSON = f'{file_dir}wikipedia.movies.json'
finished_wikiJSON = f'{finished_dir}wikipedia.movies_{timestr}.json'

# Kaggle
file_kaggleCSV = f'{file_dir}movies_metadata.csv'
finished_kaggleCSV = f'{finished_dir}movies_metadata_{timestr}.csv'
# Ratings
file_ratingsCSV = f'{file_dir}ratings.csv'
finished_ratingsCSV = f'{finished_dir}ratings_{timestr}.csv'

str_database = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"


# Main function to check if file is ready
def LoadData(fileWikiJSON, fileKaggleCSV, fileRatingsCSV):
    if ~os.path.exists(fileWikiJSON) | ~os.path.exists(file_kaggleCSV) | ~os.path.exists(file_ratingsCSV):
        print('3 movie data files detected. Proceeding with cleanup + loading')
        with open(f'{fileWikiJSON}', mode='r') as file:
            wiki_movies_raw = json.load(file)
        
        # Filter
        wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
        
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        df_wiki_movies = pd.DataFrame(clean_movies)
        
        df_kaggle_metadata = pd.read_csv(f'{fileKaggleCSV}')
        
        df_ratings = pd.read_csv(f'{fileRatingsCSV}')
        
        CleanData(df_wiki_movies, df_kaggle_metadata, df_ratings)
        
    else:
        print('Movie files not detected')

   
def CleanData(df_wiki, df_kag, df_rate):
    
    try:
        df_wiki['imdb_id'] = df_wiki['imdb_link'].str.extract(r'(tt\d{7})')
        df_wiki.drop_duplicates(subset='imdb_id', inplace=True)
    
        df_wiki.head()
        wiki_columns_to_keep = [column for column in df_wiki.columns if df_wiki[column].isnull().sum() < len(df_wiki) * 0.9]
        df_wiki = df_wiki[wiki_columns_to_keep]
    
        box_office = df_wiki['Box office'].dropna()
        box_office[box_office.map(lambda x: type(x) != str)]
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        
        form_one = r'\$\d+\.?\d*\s*[mb]illion'
        box_office.str.contains(form_one, flags=re.IGNORECASE).sum()
        form_two = r'\$\d{1,3}(?:,\d{3})+'
        box_office.str.contains(form_two, flags=re.IGNORECASE).sum()
        matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)
        box_office[~matches_form_one & ~matches_form_two]
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        box_office.str.extract(f'({form_one}|{form_two})')
        
        df_wiki['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        df_wiki.drop('Box office', axis=1, inplace=True)
        budget = df_wiki['Budget'].dropna()
        
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        
        matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
        budget[~matches_form_one & ~matches_form_two]
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        budget[~matches_form_one & ~matches_form_two]
        
        
        df_wiki['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        df_wiki['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        df_wiki.drop('Budget', axis=1, inplace=True)
        release_date = df_wiki['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        
        
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
        df_wiki['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        running_time = df_wiki['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE).sum()
        running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE) != True]
        running_time[running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE) != True]
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        df_wiki['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        df_wiki.drop('Running time', axis=1, inplace=True)
    
        
        # Kaggle clean up
        df_kag[~df_kag['adult'].isin(['True','False'])]
        df_kag = df_kag[df_kag['adult'] == 'False'].drop('adult',axis='columns')
        
        df_kag['video'] = df_kag['video'] == 'True'
        df_kag['budget'] = df_kag['budget'].astype(int)
        df_kag['id'] = pd.to_numeric(df_kag['id'], errors='raise')
        df_kag['popularity'] = pd.to_numeric(df_kag['popularity'], errors='raise')
    
        pd.to_datetime(df_rate['timestamp'], unit='s')    
        df_rate['timestamp'] = pd.to_datetime(df_rate['timestamp'], unit='s')
        movies_df = pd.merge(df_wiki, df_kag, on='imdb_id', suffixes=['_wiki','_kaggle'])
        
        
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
        movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
        movies_df['original_language'].value_counts(dropna=False)
        
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)    
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        for col in movies_df.columns:
            lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
            value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
            num_values = len(value_counts)
            if num_values == 1:
                print(col)
    
        movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
        
    
        rating_counts = df_rate.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
    
    
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        
        # UploadToDatabase(str_database, movies_df)
        print('Data clean + upload complete')
    except Exception as e:
        print(e)
    global bool_success
    bool_success = True

def UploadToDatabase(str_dbstring, dataframe):
    db_string = str_dbstring
    engine = create_engine(db_string)
    try:
        dataframe.to_sql(name='movies', con=engine, if_exists='replace')
    except:
        print('Incorrect address or port number')

    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)
    
        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
    print(f'Finished. {time.time() - start_time} total seconds elapsed')

def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)
    
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan
    
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie

        
        
# Main Program
try:
    os.mkdir(finished_dir)
except OSError:
    print ("Creation of the directory %s failed (it already exists)" % finished_dir)
else:
    print ("Successfully created the directory %s " % finished_dir)    

LoadData(file_wikiJSON, file_kaggleCSV, file_ratingsCSV)

if bool_success == True:
    shutil.move(file_wikiJSON, finished_wikiJSON)
    shutil.move(file_kaggleCSV, finished_kaggleCSV)
    shutil.move(file_ratingsCSV, finished_ratingsCSV)