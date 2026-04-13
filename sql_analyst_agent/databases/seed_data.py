"""
databases/seed_data.py
======================
Seeds two SQLite databases with realistic e-commerce data:
  - sales.db  : orders, products, customers, regions
  - hr.db     : employees, departments, salaries, performance

Run this once before using the agent:
    python databases/seed_data.py
"""

import sqlite3
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────
# SALES DATABASE
# ─────────────────────────────────────────────────────────────

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Beauty", "Automotive"]
REGIONS     = ["North", "South", "East", "West", "Central"]
STATUSES    = ["completed", "refunded", "pending", "cancelled"]

PRODUCTS = [
    ("Wireless Headphones",   "Electronics",   79.99),
    ("Bluetooth Speaker",     "Electronics",  49.99),
    ("Laptop Stand",          "Electronics",  34.99),
    ("USB-C Hub",             "Electronics",  29.99),
    ("Mechanical Keyboard",   "Electronics",  89.99),
    ("Running Shoes",         "Sports",       65.00),
    ("Yoga Mat",              "Sports",       28.50),
    ("Protein Powder",        "Sports",       42.00),
    ("Water Bottle",          "Sports",       18.99),
    ("Resistance Bands",      "Sports",       14.99),
    ("Winter Jacket",         "Clothing",     110.00),
    ("Denim Jeans",           "Clothing",     55.00),
    ("Graphic T-Shirt",       "Clothing",     22.00),
    ("Sneakers",              "Clothing",      80.00),
    ("Wool Scarf",            "Clothing",     30.00),
    ("Garden Hose",           "Home & Garden", 25.00),
    ("Potting Soil",          "Home & Garden", 12.00),
    ("Tool Kit",              "Home & Garden", 45.00),
    ("Throw Pillow",          "Home & Garden", 19.99),
    ("Scented Candle",        "Home & Garden",  9.99),
    ("Python Crash Course",   "Books",        29.99),
    ("Data Science Handbook", "Books",        34.99),
    ("Fiction Novel",         "Books",        14.99),
    ("Children's Story Set",  "Books",        24.99),
    ("LEGO Set",              "Toys",         59.99),
    ("Board Game",            "Toys",         39.99),
    ("Action Figure",         "Toys",         19.99),
    ("Face Moisturizer",      "Beauty",       28.00),
    ("Hair Serum",            "Beauty",       22.00),
    ("Car Phone Mount",       "Automotive",   17.99),
]


def seed_sales_db():
    db_path = os.path.join(BASE_DIR, "sales.db")
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # ── Tables ──────────────────────────────────────────────
    cur.executescript("""
    DROP TABLE IF EXISTS customers;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS order_items;

    CREATE TABLE customers (
        customer_id   INTEGER PRIMARY KEY,
        name          TEXT    NOT NULL,
        email         TEXT    UNIQUE NOT NULL,
        region        TEXT    NOT NULL,
        signup_date   TEXT    NOT NULL
    );

    CREATE TABLE products (
        product_id    INTEGER PRIMARY KEY,
        name          TEXT    NOT NULL,
        category      TEXT    NOT NULL,
        unit_price    REAL    NOT NULL
    );

    CREATE TABLE orders (
        order_id      INTEGER PRIMARY KEY,
        customer_id   INTEGER NOT NULL,
        order_date    TEXT    NOT NULL,
        status        TEXT    NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE order_items (
        item_id       INTEGER PRIMARY KEY,
        order_id      INTEGER NOT NULL,
        product_id    INTEGER NOT NULL,
        quantity      INTEGER NOT NULL,
        unit_price    REAL    NOT NULL,
        FOREIGN KEY (order_id)   REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """)

    # ── Products ─────────────────────────────────────────────
    for pid, (name, cat, price) in enumerate(PRODUCTS, start=1):
        cur.execute(
            "INSERT INTO products VALUES (?,?,?,?)",
            (pid, name, cat, price)
        )

    # ── Customers (150) ──────────────────────────────────────
    for cid in range(1, 151):
        signup = fake.date_between(start_date="-3y", end_date="-6m")
        cur.execute(
            "INSERT INTO customers VALUES (?,?,?,?,?)",
            (cid, fake.name(), fake.unique.email(),
             random.choice(REGIONS), str(signup))
        )

    # ── Orders + Items (600 orders) ──────────────────────────
    item_id    = 1
    start_date = datetime(2023, 1, 1)
    end_date   = datetime(2024, 12, 31)

    for oid in range(1, 601):
        cid        = random.randint(1, 150)
        order_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days)
        )
        status     = random.choices(
            STATUSES, weights=[70, 10, 15, 5]
        )[0]
        cur.execute(
            "INSERT INTO orders VALUES (?,?,?,?)",
            (oid, cid, str(order_date.date()), status)
        )

        # 1–4 items per order
        n_items = random.randint(1, 4)
        chosen  = random.sample(PRODUCTS, k=min(n_items, len(PRODUCTS)))
        for (pname, pcat, pprice) in chosen:
            pid = PRODUCTS.index((pname, pcat, pprice)) + 1
            qty = random.randint(1, 3)
            cur.execute(
                "INSERT INTO order_items VALUES (?,?,?,?,?)",
                (item_id, oid, pid, qty, pprice)
            )
            item_id += 1

    conn.commit()
    conn.close()
    print(f"✅  sales.db created at {db_path}")


# ─────────────────────────────────────────────────────────────
# HR DATABASE
# ─────────────────────────────────────────────────────────────

DEPARTMENTS  = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Customer Support"]
JOB_TITLES   = {
    "Engineering":       ["Software Engineer", "Senior Engineer", "Tech Lead", "Architect"],
    "Sales":             ["Sales Rep", "Account Executive", "Sales Manager", "VP Sales"],
    "Marketing":         ["Marketing Analyst", "Content Strategist", "Brand Manager", "CMO"],
    "HR":                ["HR Coordinator", "Recruiter", "HR Manager", "CHRO"],
    "Finance":           ["Financial Analyst", "Accountant", "Finance Manager", "CFO"],
    "Operations":        ["Operations Analyst", "Logistics Coordinator", "Ops Manager", "COO"],
    "Customer Support":  ["Support Agent", "Team Lead", "Support Manager", "Director"],
}
SALARY_BANDS = {
    "Engineering":      (75000, 160000),
    "Sales":            (55000, 130000),
    "Marketing":        (55000, 120000),
    "HR":               (50000, 100000),
    "Finance":          (60000, 130000),
    "Operations":       (50000, 110000),
    "Customer Support": (40000, 85000),
}
RATINGS      = ["Exceeds Expectations", "Meets Expectations", "Needs Improvement"]


def seed_hr_db():
    db_path = os.path.join(BASE_DIR, "hr.db")
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    cur.executescript("""
    DROP TABLE IF EXISTS departments;
    DROP TABLE IF EXISTS employees;
    DROP TABLE IF EXISTS salaries;
    DROP TABLE IF EXISTS performance_reviews;

    CREATE TABLE departments (
        dept_id     INTEGER PRIMARY KEY,
        name        TEXT    NOT NULL,
        manager_id  INTEGER
    );

    CREATE TABLE employees (
        emp_id      INTEGER PRIMARY KEY,
        name        TEXT    NOT NULL,
        email       TEXT    UNIQUE NOT NULL,
        dept_id     INTEGER NOT NULL,
        job_title   TEXT    NOT NULL,
        hire_date   TEXT    NOT NULL,
        status      TEXT    NOT NULL DEFAULT 'active',
        FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
    );

    CREATE TABLE salaries (
        salary_id   INTEGER PRIMARY KEY,
        emp_id      INTEGER NOT NULL,
        base_salary REAL    NOT NULL,
        bonus       REAL    NOT NULL DEFAULT 0,
        effective_date TEXT NOT NULL,
        FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
    );

    CREATE TABLE performance_reviews (
        review_id   INTEGER PRIMARY KEY,
        emp_id      INTEGER NOT NULL,
        review_year INTEGER NOT NULL,
        rating      TEXT    NOT NULL,
        score       REAL    NOT NULL,
        FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
    );
    """)

    # ── Departments ───────────────────────────────────────────
    for did, dname in enumerate(DEPARTMENTS, start=1):
        cur.execute("INSERT INTO departments (dept_id, name) VALUES (?,?)", (did, dname))

    # ── Employees (180) ───────────────────────────────────────
    salary_id = 1
    review_id = 1

    for eid in range(1, 181):
        dept    = random.choice(DEPARTMENTS)
        did     = DEPARTMENTS.index(dept) + 1
        title   = random.choice(JOB_TITLES[dept])
        hire    = fake.date_between(start_date="-8y", end_date="-1y")
        status  = random.choices(["active", "inactive"], weights=[85, 15])[0]
        cur.execute(
            "INSERT INTO employees VALUES (?,?,?,?,?,?,?)",
            (eid, fake.name(), fake.unique.email(), did, title, str(hire), status)
        )

        # Salary
        lo, hi  = SALARY_BANDS[dept]
        salary  = round(random.uniform(lo, hi), 2)
        bonus   = round(salary * random.uniform(0.0, 0.20), 2)
        cur.execute(
            "INSERT INTO salaries VALUES (?,?,?,?,?)",
            (salary_id, eid, salary, bonus, str(hire))
        )
        salary_id += 1

        # 1–3 performance reviews
        for year in random.sample([2022, 2023, 2024], k=random.randint(1, 3)):
            rating = random.choices(RATINGS, weights=[30, 55, 15])[0]
            score  = round(random.uniform(2.5, 5.0), 1)
            cur.execute(
                "INSERT INTO performance_reviews VALUES (?,?,?,?,?)",
                (review_id, eid, year, rating, score)
            )
            review_id += 1

    conn.commit()
    conn.close()
    print(f"✅  hr.db created at {db_path}")


if __name__ == "__main__":
    print("🌱 Seeding databases...")
    seed_sales_db()
    seed_hr_db()
    print("\n🎉 Both databases ready. You can now run the agent.")
