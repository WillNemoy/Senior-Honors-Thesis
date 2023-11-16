from google.colab import files

from bs4 import BeautifulSoup
import pandas as pd

import requests


def get_apps(url):

  response = requests.get(url)

  soup = BeautifulSoup(response.text, 'html.parser')
  elements = soup.find_all('a', class_ = 'we-lockup targeted-link')

  app_urls = []
  app_names = []
  app_ids = []
  app_categories = []
  app_category_urls = []
  app_free_or_paids = []
  app_page_ranks = []

  for i, element in enumerate(elements):
    app_url = element.get('href')
    app_name = app_url.split('/app/')[-1].split('/id')[0]
    app_id = app_url.split('/id')[-1]
    app_category = url.split('/iphone/')[-1].split('/')[0]
    app_category_url = url
    app_free_or_paid = url.split('?chart=top-')[-1]
    app_page_rank = i + 1

    app_urls.append(app_url)
    app_names.append(app_name)
    app_ids.append(app_id)
    app_categories.append(app_category)
    app_category_urls.append(app_category_url)
    app_free_or_paids.append(app_free_or_paid)
    app_page_ranks.append(app_page_rank)

  df = pd.DataFrame({'url': app_urls,
                     'name': app_names,
                     'id': app_ids,
                     'category': app_categories,
                     'category_url': app_category_urls,
                     'cost': app_free_or_paids,
                     'category_cost_rank': app_page_ranks})

  return df

df_categories = pd.read_excel('/content/App Store Categories.xlsx')
all_dfs = []

for url in df_categories['App Category URLs']:

  url_free = f'{url}?chart=top-free'
  url_paid = f'{url}?chart=top-paid'

  all_dfs.append(get_apps(url_free))
  all_dfs.append(get_apps(url_paid))

df = pd.concat(all_dfs)
df = df.drop_duplicates(subset=['id'])
df = df.reset_index(drop=True)
df.to_excel('App Store Apps (11-11-2023).xlsx')
files.download('App Store Apps (11-11-2023).xlsx')


def itunesRSS(appId):

    dfs = []

    for i in range(10):
        try:
            page = i + 1

            apple_request = ("https://itunes.apple.com/us/rss/customerreviews/page="
                            + str(page)
                            + "/id="
                            + str(appId)
                            + "/sortBy=mostHelpful/json")

            apple_Json = requests.get(apple_request).json()

            df = pd.DataFrame(apple_Json["feed"]["entry"])

            #fix up the df (convert json to content)
            df["app_id"] = appId

            df["uri"] = df["author"].apply(lambda x: x["uri"]["label"])
            df["author"] = df["author"].apply(lambda x: x["name"]["label"])
            df["updated"] = df["updated"].apply(lambda x: x["label"])
            df["im:rating"] = df["im:rating"].apply(lambda x: x["label"])
            df = df.rename(columns={"im:rating": "rating"})

            df["im:version"] = df["im:version"].apply(lambda x: x["label"])
            df = df.rename(columns={"im:version": "version"})

            df["id"] = df["id"].apply(lambda x: x["label"])
            df["title"] = df["title"].apply(lambda x: x["label"])
            df["content"] = df["content"].apply(lambda x: x["label"])

            df["link"] = df["link"].apply(lambda x: x["attributes"]["href"])
            df["im:voteSum"] = df["im:voteSum"].apply(lambda x: x["label"])
            df = df.rename(columns={"im:voteSum": "voteSum"})

            df["im:contentType"] = df["im:contentType"].apply(lambda x: x["attributes"]["label"])
            df = df.rename(columns={"im:contentType": "contentType"})

            df["im:voteCount"] = df["im:voteCount"].apply(lambda x: x["label"])
            df = df.rename(columns={"im:voteCount": "voteCount"})


            #create a day and time column
            df["updated day"] = df["updated"].apply(lambda x: x.split("T")[0])
            df["updated time"] = df["updated"].apply(lambda x: "T" + x.split("T")[1])

            df = df.drop("updated", axis=1)

            dfs.append(df)

        except:
            break

    df_all = pd.concat(dfs).reset_index()
    df_all = df_all.drop("index", axis=1)

    return df_all

def get_reviews(start, end):

  all_dfs = []
  failed_ids = []

  for i, id in enumerate(df_apps['id'][start:end]):
    try:
      all_dfs.append(itunesRSS(id))
    except:
      print(f'Failed id {id}, at position {i + start}')
      failed_ids.append({'id':id, 'position':i + start})

  df = pd.concat(all_dfs)
  df = df.drop_duplicates(subset=['id'])
  df = df.reset_index(drop=True)

  df.to_pickle(f"App Store Reviews (11-12-2023) ({start}-{end}).pkl")
  files.download(f"App Store Reviews (11-12-2023) ({start}-{end}).pkl")

  df_failed = pd.DataFrame(failed_ids)
  df_failed.to_excel(f"App Store Reviews Failed (11-12-2023) ({start}-{end}).xlsx")
  files.download(f"App Store Reviews Failed (11-12-2023) ({start}-{end}).xlsx")

get_reviews(0, 6000)