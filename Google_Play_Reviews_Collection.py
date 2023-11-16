from google_play_scraper import Sort, reviews, search
from google.colab import files

import pandas as pd
import json
import time
from datetime import datetime



# https://support.google.com/googleplay/android-developer/answer/9859673?hl=en#zippy=%2Capps%2Cgames
# 47 categories
categories = ['art and design', 'auto and vehicles', 'beauty', 'books and reference'
              'business', 'comics', 'communications', 'dating', 'education'
              'entertainment', 'events', 'finance', 'food and drink',
              'health and fitness', 'house and home', 'libraries and demo',
              'lifestyle', 'maps and navigation', 'medical', 'music and audio',
              'news and magazines', 'parenting', 'personalization', 'photography',
              'productivity', 'shopping', 'social', 'sports', 'tools', 'travel and local',
              'video players and editors', 'weather', 'action game', 'adventure game',
              'arcade game', 'board game', 'card game', 'casino game', 'casual game',
              'educational game', 'music game', 'puzzle game', 'racing game',
              'role playing game', 'simulation game', 'sports game', 'strategy game', 'trivia game',
              'word game']

def search_store(search_term):

  result = search(
      search_term,
      lang="en",  # defaults to 'en'
      country="us",  # defaults to 'us'
      n_hits=30  # defaults to 30 (= Google's maximum)
  )

  result_json = json.dumps(result)
  df = pd.read_json(result_json)

  df['searchTerm'] = search_term

  return df


all_dfs = []

for category in categories:
  df = search_store(category)
  all_dfs.append(df)
  time.sleep(1)

df = pd.concat(all_dfs)
df = df.drop_duplicates(subset=['appId'])
# Maybe include a line here to reset the axis, like: df.reset_index(drop=True)
df.to_excel('Google Play Apps (10-31-2023).xlsx')



def app_reviews(app_id):

  result, continuation_token = reviews(
    app_id,
    lang='en', # defaults to 'en'
    country='us', # defaults to 'us'
    sort=Sort.MOST_RELEVANT, # defaults to Sort.NEWEST
    count=1000, # defaults to 100
)

  # In result, 'at' and 'repliedAt' are datetime objects, which must be converted to strings
  for review in result:
    date_time = review['at'].strftime('%Y-%m-%d %H:%M:%S')
    review['at'] = date_time

    if review['repliedAt'] != None:
      date_time = review['repliedAt'].strftime('%Y-%m-%d %H:%M:%S')
      review['repliedAt'] = date_time


  result_json = json.dumps(result)
  df = pd.read_json(result_json)

  df['app_id'] = app_id

  return df

all_dfs = []

for app in df_apps['appId'][501:]:
  df = app_reviews(app)
  all_dfs.append(df)
  time.sleep(1)

df = pd.concat(all_dfs)
df.to_excel('Google Play Reviews (501-) (11-7-2023).xlsx')
files.download('Google Play Reviews (501-) (11-7-2023).xlsx')
