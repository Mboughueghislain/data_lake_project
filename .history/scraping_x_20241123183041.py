import tweepy
import pandas as pd

# Vos clés API Twitter
API_KEY = "Pl03jEemr5ZjNp0yx7KcwN0cF"
API_SECRET = "xlo2pqUFic8i7WEgALejeV64EzWB4P6sNAmfE5AjwJgPgiNG9U"
ACCESS_TOKEN = "1504573340547170311-5gqsUe1xToYpEyhhlQ5bjEipe1Uu58"
ACCESS_TOKEN_SECRET = "PggfF5Wcbr8CNkxbpKxzCle6H7KqjVRbS1bU34lVJV8c3"

# Authentification
auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

# Paramètres de recherche
query = "Tesla"  # Mot-clé ou hashtag à chercher
max_tweets = 100  # Nombre maximal de tweets

# Récupération des tweets
tweets = []
for tweet in tweepy.Cursor(api.search_tweets, q=query, lang="en", tweet_mode="extended").items(max_tweets):
    tweets.append([tweet.created_at, tweet.user.screen_name, tweet.full_text])

# Sauvegarder dans un DataFrame
df = pd.DataFrame(tweets, columns=["Date", "User", "Tweet"])

# Afficher un aperçu
print(df.head())

# Sauvegarder les tweets dans un fichier CSV
output_file = "tesla_tweets_tweepy.csv"
df.to_csv(output_file, index=False, encoding="utf-8")
print(f"Les tweets sur Tesla ont été sauvegardés dans {output_file}")