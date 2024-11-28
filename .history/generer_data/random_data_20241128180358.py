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
    
    
    
    import random
import csv
from faker import Faker

# Initialize Faker instance
fake = Faker("fr_FR")

# Generate 75 Customers (just for the sake of referencing customer data in transactions)
customers = [
    {
        "id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address().replace("\n", ", ")
    }
    for i in range(1, 76)  # Generate 75 customers
]

# Generate 20 Products (just for the sake of referencing products in transactions)
products = [
    {
        "id": i,
        "product_name": fake.word().capitalize(),
        "description": fake.sentence(),
        "price": round(random.uniform(10, 500), 2),
        "available_stock": random.randint(20, 200),
        "category": random.choice(["Electronics", "Books", "Clothing", "Home", "Toys"])
    }
    for i in range(1, 21)  # Generate 20 products
]

# Generate Transactions
transactions = []

for t_id in range(1, 76):  # 75 transactions
    customer_id = random.randint(1, 75)
    shipping_fee = round(random.uniform(5, 20), 2)
    discount = round(random.uniform(0, 50), 2)

    # Choose 1-5 random products for this transaction
    selected_products = random.sample(products, random.randint(1, 5))
    total_amount = 0
    transaction_details = []

    for product in selected_products:
        quantity = random.randint(1, 5)
        unit_price = product["price"]
        total_price = round(quantity * unit_price, 2)

        # Add to transaction details
        transaction_details.append({
            "product_id": product["id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price
        })

        # Decrease stock in product
        product["available_stock"] = max(product["available_stock"] - quantity, 0)
        total_amount += total_price

    total_amount = round(total_amount + shipping_fee - discount, 2)

    transactions.append({
        "transaction_id": t_id,
        "customer_id": customer_id,
        "customer_name": f"{customers[customer_id - 1]['first_name']} {customers[customer_id - 1]['last_name']}",
        "email": customers[customer_id - 1]["email"],
        "phone": customers[customer_id - 1]["phone"],
        "address": customers[customer_id - 1]["address"],
        "transaction_status": random.choice(["Completed", "Pending", "Cancelled"]),
        "payment_method": random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
        "total_amount": total_amount,
        "currency": "USD",
        "shipping_fee": shipping_fee,
        "discount": discount,
        "transaction_details": transaction_details
    })

# Save the Transactions to a single CSV
with open("transactions_complete.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = [
        "transaction_id", "customer_id", "customer_name", "email", "phone", "address", 
        "transaction_status", "payment_method", "total_amount", "currency", 
        "shipping_fee", "discount", "transaction_details"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for transaction in transactions:
        # Flatten the transaction_details into a string format
        transaction["transaction_details"] = "; ".join(
            [f"{item['product_name']} (ID: {item['product_id']}), Quantity: {item['quantity']}, Total: {item['total_price']}" 
             for item in transaction["transaction_details"]]
        )
        writer.writerow(transaction)

print("CSV file for the complete transactions has been successfully generated.")