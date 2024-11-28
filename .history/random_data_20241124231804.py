import random
from faker import Faker

# Initialize Faker instance
fake = Faker()

# Generate 75 Customers
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

# Generate 20 Products
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

# Generate Transactions and Transaction Details
transactions = []
transaction_details = []

for t_id in range(1, 76):  # 75 transactions
    customer_id = random.randint(1, 75)
    shipping_fee = round(random.uniform(5, 20), 2)
    discount = round(random.uniform(0, 50), 2)

    # Choose 1-5 random products for this transaction
    selected_products = random.sample(products, random.randint(1, 5))
    total_amount = 0

    for product in selected_products:
        quantity = random.randint(1, 5)
        unit_price = product["price"]
        total_price = round(quantity * unit_price, 2)

        # Add to transaction details
        transaction_details.append({
            "transaction_id": t_id,
            "product_id": product["id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total": total_price
        })

        # Decrease stock in product
        product["available_stock"] = max(product["available_stock"] - quantity, 0)  # Ensure stock doesn't go negative
        total_amount += total_price

    total_amount = round(total_amount + shipping_fee - discount, 2)
    transactions.append({
        "id": t_id,
        "customer_id": customer_id,
        "total_amount": total_amount,
        "currency": "USD",
        "payment_method": random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
        "transaction_status": random.choice(["Completed", "Pending", "Cancelled"]),
        "shipping_address": customers[customer_id - 1]["address"],
        "promo_code": random.choice([None, "DISCOUNT10", "SUMMER2024"]),
        "transaction_type": "Purchase",
        "shipping_fee": shipping_fee,
        "discount": discount
    })

# Generate SQL for each table
customer_sql = "INSERT INTO customers (id, first_name, last_name, email, phone, address) VALUES\n"
customer_sql += ",\n".join([
    f"({c['id']}, '{c['first_name']}', '{c['last_name']}', '{c['email']}', '{c['phone']}', '{c['address'].replace('\'', '\\\'')}')"
    for c in customers
]) + ";"

product_sql = "INSERT INTO products (id, product_name, description, price, available_stock, category) VALUES\n"
product_sql += ",\n".join([
    f"({p['id']}, '{p['product_name']}', '{p['description'].replace('\'', '\\\'')}', {p['price']}, {p['available_stock']}, '{p['category']}')"
    for p in products
]) + ";"

transaction_sql = (
    "INSERT INTO transactions (id, customer_id, total_amount, currency, payment_method, "
    "transaction_status, shipping_address, promo_code, transaction_type, shipping_fee, discount) VALUES\n"
)
transaction_sql += ",\n".join([
    (
        f"({t['id']}, {t['customer_id']}, {t['total_amount']}, '{t['currency']}', '{t['payment_method']}', "
        f"'{t['transaction_status']}', '{t['shipping_address'].replace(\"'\", \"\\'\")}', "
        f"{'NULL' if t['promo_code'] is None else f'\"{t[\"promo_code\"]}\"'}, "
        f"'{t['transaction_type']}', {t['shipping_fee']}, {t['discount']})"
    )
    for t in transactions
]) + ";"


transaction_detail_sql = (
    "INSERT INTO transaction_details (transaction_id, product_id, quantity, unit_price, total) VALUES\n"
)
transaction_detail_sql += ",\n".join([
    f"({td['transaction_id']}, {td['product_id']}, {td['quantity']}, {td['unit_price']}, {td['total']})"
    for td in transaction_details
]) + ";"

# Output SQL statements
print("-- Customers Table")
print(customer_sql)
print("\n-- Products Table")
print(product_sql)
print("\n-- Transactions Table")
print(transaction_sql)
print("\n-- Transaction Details Table")
print(transaction_detail_sql)
