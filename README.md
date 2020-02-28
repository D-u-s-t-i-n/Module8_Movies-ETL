# Module8_Movies-ETL

## Module Notes

import json
import pandas as pd
import numpy as np
file_dir = 'C:/Users/Username/DataBootcamp/'
f'{file_dir}filename'

with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
len(wiki_movies_raw)

# First 5 records
wiki_movies_raw[:5]

# Last 5 records
wiki_movies_raw[-5:]

# Some records in the middle
wiki_movies_raw[3600:3605]

## Kaggle

kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
ratings = pd.read_csv(f'{file_dir}ratings.csv')


# Inspect Plan Execute
wiki_movies_df = pd.DataFrame(wiki_movies_raw)
wiki_movies_df.head()
wiki_movies_df.columns.tolist()

if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie

wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie]
len(wiki_movies)
# 7,080

wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
# Lamda
square = lambda x: x * x
square(5)

def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    return movie
    
wiki_movies_df[wiki_movies_df['Arabic'].notnull()]
