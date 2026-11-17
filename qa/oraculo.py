#!/usr/bin/env python3
"""Oráculo independiente: recalcula en Python puro (sin Spark) lo que el pipeline
debe producir a partir de los CSV. Uso: python3 qa/oraculo.py > qa/esperado.json"""
import csv, json, os, re
from datetime import datetime, date
B = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "landing")
rd = lambda p: list(csv.DictReader(open(os.path.join(B, p))))
cust = rd("initial/customers/landing/customers_batch.csv")
prod = rd("initial/products/landing/products_batch.csv")
price = rd("initial/gross_price/landing/prices_batch.csv")
o_ini = rd("initial/orders/landing/orders_batch.csv")
o_inc = rd("incremental/orders/landing/orders_incremental_batch.csv")
m = {"bronze.customers": len(cust), "bronze.products": len(prod), "bronze.gross_price": len(price)}

# customers
MAP = {"Bengaluruu": "Bengaluru", "Bengalore": "Bengaluru", "bengaluru": "Bengaluru", "BENGALURU": "Bengaluru",
       "Hyderabadd": "Hyderabad", "Hyderbad": "Hyderabad", "hyderabad": "Hyderabad",
       "NewDelhi": "New Delhi", "NewDheli": "New Delhi", "NewDelhee": "New Delhi", "new delhi": "New Delhi"}
FIX = {"789403": "New Delhi", "789420": "Bengaluru", "789521": "Hyderabad", "789603": "Hyderabad"}
sc = {}
for r in cust:
    if r["customer_id"] in sc: continue
    c = MAP.get(r["city"].strip(), r["city"].strip())
    c = c if c in ("Bengaluru", "Hyderabad", "New Delhi") else None
    sc[r["customer_id"]] = c or FIX.get(r["customer_id"]) or "Sin ciudad"
m["silver.customers"] = len(sc); m["gold.dim_customers"] = len(sc) + 1
m["cities"] = {}
for c in sc.values(): m["cities"][c] = m["cities"].get(c, 0) + 1

# products
sp = {}
for r in prod:
    sp.setdefault(r["product_id"], r)
m["silver.products"] = m["gold.dim_products"] = len(sp)
m["products_999999"] = sum(1 for k in sp if not k.isdigit())
cat_of = {}; m["categories"] = {}
for k, r in sp.items():
    cat = re.sub("(?i)protien", "Protein", " ".join(w[:1].upper() + w[1:].lower() for w in r["category"].split(" ")))
    cat_of[k] = cat; m["categories"][cat] = m["categories"].get(cat, 0) + 1
valid_p = {k for k in sp if k.isdigit()}

# pricing
def pdate(s, fmts):
    for f in fmts:
        try: return datetime.strptime(s, f).date()
        except ValueError: pass
    return None
sp_price = {}
for r in price:
    k = (r["product_id"], pdate(r["month"], ["%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]))
    sp_price[k] = max(sp_price.get(k, 0), float(r["price"]))
m["silver.gross_price"] = m["gold.dim_pricing"] = len(sp_price)
m["gold.dim_date"] = 365

# orders
def clean(rows):
    out = {}
    for r in rows:
        if r["order_qty"] == "": continue
        cid = r["customer_id"] if r["customer_id"].isdigit() else "999999"
        d = pdate(re.sub(r"^[A-Za-z]+,\s*", "", r["order_placement_date"]), ["%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%B %d, %Y"])
        if r["product_id"] not in valid_p: continue
        out[(r["order_id"], d, cid, r["product_id"], int(r["order_qty"]))] = 1
    return list(out)
def fact(rows):
    f = {}
    for oid, d, cid, pid, q in rows:
        p = sp_price.get((pid, date(d.year, d.month, 1)))
        k = (oid, d, cid, pid)
        assert k not in f, "llave duplicada con distinta qty"
        f[k] = (q, p, round(q * p + 1e-9, 2) if p is not None else None)
    return f
s_ini = clean(o_ini); s_inc = clean(o_inc)
m["bronze.orders_initial"] = len(o_ini); m["bronze.orders"] = len(o_ini) + len(o_inc)
m["silver.orders_initial"] = len(s_ini)
f0 = fact(s_ini); fi = fact(s_inc)
tot = lambda f: {"rows": len(f), "units": sum(v[0] for v in f.values()), "revenue": round(sum(v[2] or 0 for v in f.values()), 2)}
m["fact_v0"] = tot(f0)
ins = sum(1 for k in fi if k not in f0); upd = sum(1 for k in fi if k in f0 and f0[k] != fi[k])
m["merge"] = {"staging_rows": len(fi), "ins": ins, "upd": upd}
f1 = dict(f0); f1.update(fi)
m["fact_now"] = dict(tot(f1), null_price=sum(1 for v in f1.values() if v[1] is None), orders=len({k[0] for k in f1}))
m["silver.orders"] = m["gold.fact_sales"] = len(f1)
m["orders_cust_999999"] = sum(1 for k in f1 if k[2] == "999999")
ds = [k[1] for k in f1]; m["orders_min_max"] = [str(min(ds)), str(max(ds))]; m["orders_null_dates"] = sum(1 for d in ds if d is None)
m["by_category"] = {}; m["by_month"] = {}
for k, v in f1.items():
    c = cat_of[k[3]]; m["by_category"][c] = round(m["by_category"].get(c, 0) + (v[2] or 0), 2)
    mo = str(k[1])[:7]; m["by_month"][mo] = round(m["by_month"].get(mo, 0) + (v[2] or 0), 2)
print(json.dumps(m, indent=1, ensure_ascii=False))
