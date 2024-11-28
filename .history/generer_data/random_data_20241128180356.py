from datetime import datetime, timedelta
import random
import json
import subprocess
import os

# Répertoires HDFS
hdfs_json_dir = "/transactions/raw/json"
hdfs_txt_dir = "/transactions/raw/txt"
now = datetime.now()
now= now.strftime("%Y-%m-%d_%H-%M-%S") 

def generate_social_media_data():
    platforms = ["Twitter", "Facebook", "Instagram", "LinkedIn", "Reddit", "Twitch"]
    sentiments = ["positive", "negative", "neutral"]
    tags = [
        "techlife", "AlphaBook", "frustrated", "GammaBook", "studentlife", 
        "DeltaBook", "productivity", "BetaBook", "disappointed", "OmegaBook", 
        "TravelTech", "gamingPC", "parentreview", "dataScience"
    ]
    products = [
        "AlphaBook Pro 15", "GammaBook 13", "DeltaBook 14", 
        "BetaBook Ultra 17", "OmegaBook 15", "ZetaBook Air 13"
    ]
    usernames = [
        "tech_lover89", "gadget_guru", "student_life21", "office_dad", 
        "nightowl_dev", "travel_bloggers", "pc_gamer_pro", "casual_shopper",
        "data_scientist42", "mom_and_tech"
    ]

    # Générer 10 posts fictifs
    data = []
    start_time = datetime.now() - timedelta(days=10)  # Posts sur les 10 derniers jours

    for i in range(10):
        timestamp = (start_time + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
        product = random.choice(products)
        sentiment = random.choice(sentiments)
        platform = random.choice(platforms)
        username = random.choice(usernames)
        likes = random.randint(20, 300)
        comments = random.randint(5, 50)
        shares = random.randint(0, 20)

        # Générer un contenu fictif
        content_templates = {
            "positive": [
                f"Just bought a new {product} from CompTech Store! Amazing performance and sleek design. Totally worth it! #techlife",
                f"Loving my new {product}! Perfect for study and gaming. Thanks @CompTech for the student discount! 😍",
                f"Got a {product} for the office. Great machine for productivity and multitasking. Highly recommended!",
                f"Traveling light with my new {product}. Super portable and excellent battery life."
            ],
            "negative": [
                f"The {product} is an overpriced piece of junk. Battery drains so fast, and the keyboard feels cheap. Avoid at all costs!",
                f"Anyone else having issues with the {product}'s screen? Flickers randomly, and the customer service is a nightmare.",
                f"Not impressed with the {product}. Build quality is okay, but the specs are underwhelming for the price."
            ],
            "neutral": [
                f"The {product} is okay for its price. Not great, not terrible. Does the job but nothing special.",
                f"Bought the {product}. It works fine for basic tasks, but don't expect too much.",
                f"The {product} is decent but lacks innovation. Good for those on a budget."
            ]
        }

        content = random.choice(content_templates[sentiment])

        # Ajouter un post au dataset
        data.append({
            "Post ID": str(10000 + i),
            "Username": username,
            "Platform": platform,
            "Timestamp": timestamp,
            "Content": content,
            "Sentiment": sentiment,
            "Likes": likes,
            "Comments": comments,
            "Shares": shares,
            "Tags": random.sample(tags, k=2)
        })

    return data

# Générer les données fictives
social_media_data = generate_social_media_data()

# Sauvegarder les données dans un fichier texte au format CSV-like
txt_file_path = f"data/social_media_{now}.txt"
with open(txt_file_path, "w") as txt_file:
    # Écrire l'en-tête
    txt_file.write("Post ID,Username,Platform,Timestamp,Content,Sentiment,Likes,Comments,Shares,Tags\n")
    for post in social_media_data:
        line = (
            f"{post['Post ID']},"
            f"{post['Username']},"
            f"{post['Platform']},"
            f"{post['Timestamp']},"
            f"\"{post['Content']}\","  # Protéger le contenu avec des guillemets pour éviter les conflits avec des virgules
            f"{post['Sentiment']},"
            f"{post['Likes']},"
            f"{post['Comments']},"
            f"{post['Shares']},"
            f"\"{', '.join(post['Tags'])}\"\n"  # Joindre les tags avec des virgules et protéger par des guillemets
        )
        txt_file.write(line)

print(f"Les données fictives ont été sauvegardées localement dans {txt_file_path}.")

# Transférer le fichier texte dans HDFS
try:
    subprocess.run(["hdfs", "dfs", "-put", "-f", txt_file_path, hdfs_txt_dir], check=True)
    print(f"Fichier texte transféré avec succès dans HDFS : {hdfs_txt_dir}")
except subprocess.CalledProcessError as e:
    print(f"Erreur lors du transfert du fichier texte dans HDFS : {e}")

# Sauvegarder les données dans un fichier JSON
json_file_path = f"data/social_media_{now}.json"
with open(json_file_path, "w") as json_file:
    json.dump(social_media_data, json_file, indent=4)

print(f"Les données fictives ont été sauvegardées localement dans {json_file_path}.")

# Transférer le fichier JSON dans HDFS
try:
    subprocess.run(["hdfs", "dfs", "-put", "-f", json_file_path, hdfs_json_dir], check=True)
    print(f"Fichier JSON transféré avec succès dans HDFS : {hdfs_json_dir}")
except subprocess.CalledProcessError as e:
    print(f"Erreur lors du transfert du fichier JSON dans HDFS : {e}")
    
    
    
    