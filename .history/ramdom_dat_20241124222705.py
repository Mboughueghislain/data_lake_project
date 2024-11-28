import random
import faker

# Initialize Faker instance
fake = faker.Faker()

# Generate random customers
customers = []
for _ in range(40):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone = fake.phone_number()
    address = fake.address().replace("\n", ", ")
    customers.append((first_name, last_name, email, phone, address))

# Generate random products
products = []
categories = ['Electronics', 'Audio', 'Books', 'Home & Kitchen', 'Fashion']
for _ in range(20):
    product_name = fake.word().capitalize() + " " + fake.word().capitalize()
    description = fake.text(max_nb_chars=100)
    price = round(random.uniform(10, 2000), 2)
    available_stock = random.randint(10, 200)
    category = random.choice(categories)
    products.append((product_name, description, price, available_stock, category))

# Generate random transactions
transactions = []
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
statuses = ['Completed', 'Pending', 'Failed']
for i in range(75):
    customer_id = random.randint(1, 75)
    total_amount = round(random.uniform(50, 5000), 2)
    currency = random.choice(['EUR', 'USD'])
    payment_method = random.choice(payment_methods)
    transaction_status = random.choice(statuses)
    shipping_address = fake.address().replace("\n", ", ")
    promo_code = fake.word() if random.random() < 0.3 else None
    transaction_type = random.choice(['Purchase', 'Refund'])
    shipping_fee = round(random.uniform(5, 50), 2)
    discount = round(random.uniform(0, 100), 2)
    transactions.append((customer_id, total_amount, currency, payment_method, transaction_status, shipping_address, promo_code, transaction_type, shipping_fee, discount))

# Generate random transaction details
transaction_details = []
for i in range(75):
    transaction_id = random.randint(1, 75)
    product_id = random.randint(1, 75)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 500), 2)
    total = round(quantity * unit_price, 2)
    transaction_details.append((transaction_id, product_id, quantity, unit_price, total))

# Prepare the data for SQL
customer_inserts = "\n".join([f"('{c[0]}', '{c[1]}', '{c[2]}', '{c[3]}', '{c[4]}')" for c in customers])
product_inserts = "\n".join([f"('{p[0]}', '{p[1]}', {p[2]}, {p[3]}, '{p[4]}')" for p in products])
transaction_inserts = "\n".join([f"({t[0]}, {t[1]}, '{t[2]}', '{t[3]}', '{t[4]}', '{t[5]}', '{t[6] if t[6] else 'NULL'}', '{t[7]}', {t[8]}, {t[9]})" for t in transactions])
transaction_detail_inserts = "\n".join([f"({td[0]}, {td[1]}, {td[2]}, {td[3]}, {td[4]})" for td in transaction_details])

# Final SQL insert statements
customer_sql = f"INSERT INTO customers (first_name, last_name, email, phone, address)\nVALUES\n{customer_inserts};"
product_sql = f"INSERT INTO products (product_name, description, price, available_stock, category)\nVALUES\n{product_inserts};"
transaction_sql = f"INSERT INTO transactions (customer_id, total_amount, currency, payment_method, transaction_status, shipping_address, promo_code, transaction_type, shipping_fee, discount)\nVALUES\n{transaction_inserts};"
transaction_detail_sql = f"INSERT INTO transaction_details (transaction_id, product_id, quantity, unit_price, total)\nVALUES\n{transaction_detail_inserts};"

(customer_sql, product_sql, transaction_sql, transaction_detail_sql)