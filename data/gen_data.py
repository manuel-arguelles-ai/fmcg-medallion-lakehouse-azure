#!/usr/bin/env python3
"""Generador determinista de data FMCG 'sucia' para el taller solidario.

Produce dos cargas:
  data/landing/initial/     -> carga bulk (clientes, productos, precios, órdenes históricas)
  data/landing/incremental/ -> archivo nuevo de órdenes para demostrar MERGE incremental

La suciedad es intencional y mapea 1:1 con las transformaciones Silver:
  - duplicados por id
  - nombres con espacios extra / minúsculas
  - typos de ciudad (Bengaluruu, Bengalore, Hyderabadd, Hyderbad, NewDelhi, NewDheli, NewDelhee)
  - ciudades nulas (4 clientes conocidos)
  - categorías en minúscula + typo "Protien"
  - fechas en 4 formatos mezclados, órdenes con prefijo de día de semana
  - customer_id / product_id inválidos (texto, vacíos)
  - order_qty nulos (se filtran en Silver)
"""
import csv
import random
import os
from datetime import date, timedelta

random.seed(20261119)  # determinista: mismo dataset siempre

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "landing")

CITIES = ["Bengaluru", "Hyderabad", "New Delhi"]
CITY_TYPOS = {
    "Bengaluru": ["Bengaluruu", "Bengalore", "bengaluru", " BENGALURU "],
    "Hyderabad": ["Hyderabadd", "Hyderbad", "hyderabad"],
    "New Delhi": ["NewDelhi", "NewDheli", "NewDelhee", "new delhi"],
}
CATEGORIES = ["energy bars", "protien bars", "granola & cereals", "recovery dairy",
              "healthy snacks", "electrolyte mix"]
VARIANTS = ["Chocolate", "Vanilla", "Berry", "Peanut", "Mango", "Lemon", "Coffee", "Natural"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]


def w(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(header)
        wr.writerows(rows)
    print(f"{path}: {len(rows)} filas")


# ---------- customers ----------
def gen_customers(n=180):
    rows = []
    for i in range(1, n + 1):
        cid = 789000 + i
        name = random.choice([
            "Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane",
            "FitMart Retail", "GreenBasket Stores", "NutriZone", "HealthyWay Supermarkets",
            "UrbanFit Corner", "DailyFuel Outlets", "ProMeal Chains", "VitaStore",
        ]) + f" #{i:03d}"
        city = None
        if random.random() > 0.12:  # ~12% nulos
            real = random.choice(CITIES)
            city = random.choice([real] + CITY_TYPOS[real])
        # suciedad: espacios extra y casing
        if random.random() < 0.25:
            name = "  " + name.lower() + " "
        rows.append([cid, name, city])
    # duplicados intencionales
    rows += random.sample(rows, 12)
    # 4 clientes de negocio con ciudad nula confirmada (fix en Silver)
    rows += [[789403, "Sprintx Nutrition Corporate", None],
             [789420, "Zenathlete Foods HQ", None],
             [789521, "Primefuel Nutrition Direct", None],
             [789603, "Recovery Lane Express", None]]
    w(f"{BASE}/initial/customers/landing/customers_batch.csv",
      ["customer_id", "customer_name", "city"], rows)


# ---------- products ----------
def gen_products(n=60):
    rows = []
    for i in range(1, n + 1):
        pid = 500 + i if random.random() > 0.08 else f"P{i:03d}X"  # ~8% ids inválidos
        cat = random.choice(CATEGORIES)
        name = f"{cat.title().replace('Protien', 'Protien')} Bar Co {i:02d}"
        if random.random() < 0.5:
            name = name.lower()
        name = f"{name} ({random.choice(VARIANTS)})"
        rows.append([pid, name, cat])
    rows += random.sample(rows, 6)  # duplicados
    w(f"{BASE}/initial/products/landing/products_batch.csv",
      ["product_id", "product_name", "category"], rows)


def is_num(pid):
    return str(pid).strip().isdigit()


# ---------- gross_price ----------
def gen_pricing(products, n_months=6):
    rows = []
    months = [date(2026, m, 1) for m in range(1, n_months + 1)]
    for pid, _, _ in products:
        if not is_num(pid):
            continue  # ids inválidos no tienen precio
        price = round(random.uniform(1.5, 12.0), 2)
        for m in months:
            fmt = random.choice(["ymd", "dmy", "iso", "dmy2"])
            if fmt == "ymd":
                ms = m.strftime("%Y/%m/%d")
            elif fmt == "dmy":
                ms = m.strftime("%d/%m/%Y")
            elif fmt == "iso":
                ms = m.strftime("%Y-%m-%d")
            else:
                ms = m.strftime("%d-%m-%Y")
            rows.append([pid, ms, price])
    w(f"{BASE}/initial/gross_price/landing/prices_batch.csv",
      ["product_id", "month", "price"], rows)


# ---------- orders ----------
def fmt_date(d):
    fmt = random.choice(["weekday_us", "ymd", "dmy", "dmy2"])
    if fmt == "weekday_us":
        return f"{WEEKDAYS[d.weekday()]}, {MONTHS[d.month-1][:d.month-0]} {d.day:02d}, {d.year}" \
            if False else f"{WEEKDAYS[d.weekday()]}, {MONTHS[d.month-1]} {d.day:02d}, {d.year}"
    if fmt == "ymd":
        return d.strftime("%Y/%m/%d")
    if fmt == "dmy":
        return d.strftime("%d/%m/%Y")
    return d.strftime("%d-%m-%Y")


def gen_orders(products, n=5000, start=date(2026, 1, 1), days=150, tag=""):
    rows = []
    valid_ids = [p[0] for p in products if is_num(p[0])]
    for i in range(1, n + 1):
        oid = 100000 + i
        d = start + timedelta(days=random.randint(0, days - 1))
        cid = 789000 + random.randint(1, 180)
        cid = cid if random.random() > 0.05 else random.choice(["", "CUST99", "abc123"])  # inválidos
        pid = random.choice(valid_ids) if random.random() > 0.04 else f"999X{i % 9}"    # inválidos
        qty = random.randint(1, 40) if random.random() > 0.06 else ""                    # nulos
        rows.append([oid, fmt_date(d), cid, pid, qty])
    rows += random.sample(rows, max(20, n // 100))  # duplicados
    out = f"{BASE}/{'incremental' if tag else 'initial'}/orders/landing"
    name = f"orders_incremental_batch.csv" if tag else "orders_batch.csv"
    w(f"{out}/{name}", ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"], rows)


def gen_incremental(products, n_new=300, n_fix=20):
    """Lote incremental: órdenes NUEVAS (ids 200001+, 1–15 jun) + correcciones de
    cantidad sobre órdenes que ya están en la carga inicial (misma llave, otra qty).
    Usa su propio generador aleatorio: no altera la carga inicial."""
    rnd = random.Random(20261120)
    valid_ids = [p[0] for p in products if is_num(p[0])]
    fmts = ["%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"]
    rows = []
    for i in range(1, n_new + 1):
        d = date(2026, 6, 1) + timedelta(days=rnd.randint(0, 14))
        ds = (f"{WEEKDAYS[d.weekday()]}, {MONTHS[d.month-1]} {d.day:02d}, {d.year}"
              if rnd.random() < 0.25 else d.strftime(rnd.choice(fmts)))
        cid = 789000 + rnd.randint(1, 180)
        cid = cid if rnd.random() > 0.05 else rnd.choice(["", "CUST99", "abc123"])
        pid = rnd.choice(valid_ids) if rnd.random() > 0.04 else f"999X{i % 9}"
        qty = rnd.randint(1, 40) if rnd.random() > 0.06 else ""
        rows.append([200000 + i, ds, cid, pid, qty])
    rows += rnd.sample(rows, 10)  # duplicados dentro del lote
    # correcciones: filas limpias y únicas de la carga inicial, con cantidad corregida
    with open(f"{BASE}/initial/orders/landing/orders_batch.csv") as f:
        initial = list(csv.reader(f))[1:]
    seen = {}
    for r in initial:
        seen[r[0]] = seen.get(r[0], 0) + 1
    clean = [r for r in initial if seen[r[0]] == 1 and r[2].isdigit() and r[3].isdigit() and r[4] != ""]
    for r in rnd.sample(clean, n_fix):
        rows.append([r[0], r[1], r[2], r[3], int(r[4]) + 5])
    w(f"{BASE}/incremental/orders/landing/orders_incremental_batch.csv",
      ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"], rows)


if __name__ == "__main__":
    gen_customers()
    gen_products()
    products = []
    with open(f"{BASE}/initial/products/landing/products_batch.csv") as f:
        products = list(csv.DictReader(f))
    products = [(r["product_id"], r["product_name"], r["category"]) for r in products]
    gen_pricing(products)
    gen_orders(products)
    gen_incremental(products)
