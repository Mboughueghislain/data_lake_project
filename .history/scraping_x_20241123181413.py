import snscrape.modules.twitter as sntwitter
import pandas as pd

# Paramètres de recherche
query = "Tesla"  # Mot-clé ou hashtag à rechercher
max_tweets = 100  # Nombre maximal de tweets à récupérer

# Scraper les tweets
tweets = []
for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
    if i >= max_tweets:
        break
    tweets.append([tweet.date, tweet.username, tweet.content])

# Sauvegarder dans un DataFrame
df = pd.DataFrame(tweets, columns=["Date", "User", "Tweet"])

# Afficher un aperçu des tweets
print(df.head())

# Sauvegarder les tweets dans un fichier CSV
output_file = "tesla_tweets.csv"
df.to_csv(output_file, index=False, encoding="utf-8")
print(f"Les tweets sur Tesla ont été sauvegardés dans {output_file}")