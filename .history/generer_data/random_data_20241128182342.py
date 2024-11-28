# Sauvegarde des transactions au format CSV
csv_file_path = f"data/transactions_{now}.csv"
os.makedirs("data", exist_ok=True)

# Génération de transactions fictives
def generate_transactions():
    fake = Faker("fr_FR")
    
    # Génération de clients
    customers = [
        {
            "id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace("\n", ", ")
        }
        for i in range(1, 76)
    ]

    # Génération de produits
    products = [
        {
            "id": i,
            "product_name": fake.word().capitalize(),
            "description": fake.sentence(),
            "price": round(random.uniform(10, 500), 2),
            "available_stock": random.randint(20, 200),
            "category": random.choice(["Electronics", "Books", "Clothing", "Home", "Toys"])
        }
        for i in range(1, 21)
    ]

    # Génération des transactions
    transactions = []
    for t_id in range(1, 76):  # 75 transactions
        customer_id = random.randint(1, len(customers))
        shipping_fee = round(random.uniform(5, 20), 2)
        discount = round(random.uniform(0, 50), 2)

        selected_products = random.sample(products, random.randint(1, 5))
        total_amount = 0
        transaction_details = []

        for product in selected_products:
            quantity = random.randint(1, 5)
            unit_price = product["price"]
            total_price = round(quantity * unit_price, 2)

            transaction_details.append({
                "product_id": product["id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price
            })

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

    return transactions

# Générer les transactions fictives
transactions = generate_transactions()

# Écriture dans le fichier CSV
with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = [
        "transaction_id", "customer_id", "customer_name", "email", "phone", "address", 
        "transaction_status", "payment_method", "total_amount", "currency", 
        "shipping_fee", "discount", "transaction_details"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for transaction in transactions:
        # Transformer les détails des transactions en une chaîne lisible
        transaction["transaction_details"] = "; ".join(
            [f"{item['product_name']} (ID: {item['product_id']}), Quantity: {item['quantity']}, Total: {item['total_price']}" 
             for item in transaction["transaction_details"]]
        )
        writer.writerow(transaction)

print(f"Fichier CSV des transactions sauvegardé localement : {csv_file_path}")

# Transférer le fichier CSV vers HDFS
save_and_transfer_file(csv_file_path, f"{HDFS_CSV_DIR}/transactions_{now}.csv")
