#!/usr/bin/env python3
"""
Génération de données synthétiques MKADIA optimisée
avec aperçu en console (preview) même en mode full.
LLM (Ollama) utilisé uniquement pour exemples réalistes.
"""

import argparse
import json
import os
import random
import string
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from faker import Faker
import requests

fake = Faker("fr_FR")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi3:mini")
JSON_BLOCK_REGEX = re.compile(r"```(?:json)?\s*(\[[\s\S]+?\])\s*```", re.MULTILINE)

PREVIEW_LIMIT = 5  # Nombre d'éléments à afficher en console

@dataclass(frozen=True)
class GenerationConfig:
    categories: int
    products_min: int
    products_max: int
    users: int
    orders: int

SAMPLE_CONFIG = GenerationConfig(categories=2, products_min=2, products_max=2, users=2, orders=2)
FULL_CONFIG = GenerationConfig(categories=15, products_min=15, products_max=30, users=100, orders=500)

BATCH_SIZE_PRODUCTS = 5
BATCH_SIZE_USERS = 10

# ------------------- Utilitaires -------------------
def slugify(value: str) -> str:
    value = (value or "").lower()
    value = value.replace("'", "").replace("’", "")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")

def call_ollama(prompt: str, system_prompt: Optional[str] = None) -> str:
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    payload = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": False,
        "options": {"temperature": 0.7, "top_p": 0.9, "max_output_tokens": 200},
    }
    try:
        response = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=300)
        response.raise_for_status()
        data = response.json()
        if "message" in data:
            return data["message"].get("content", "").strip()
        return data.get("response", "").strip()
    except Exception as exc:
        print(f"Erreur Ollama: {exc}")
        return ""

def extract_json_array(response: str) -> Optional[List[Any]]:
    if not response:
        return None
    block_match = JSON_BLOCK_REGEX.search(response)
    if block_match:
        json_str = block_match.group(1)
    else:
        start = response.find("[")
        end = response.rfind("]") + 1
        if start != -1 and end > start:
            json_str = response[start:end]
        else:
            return None
    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        return None
    return None

def request_llm_json(prompt: str, system_prompt: Optional[str] = None, retries: int = 2) -> Optional[List[Any]]:
    for attempt in range(retries):
        response = call_ollama(prompt, system_prompt)
        parsed = extract_json_array(response)
        if parsed:
            return parsed
        if attempt < retries - 1:
            print("⚠️  Réponse LLM invalide, nouvelle tentative...")
    return None

def preview(title: str, items: List[Dict[str, Any]], limit: int = PREVIEW_LIMIT) -> None:
    print(f"\n📄 Aperçu {title} (total: {len(items)})")
    for item in items[:limit]:
        print(json.dumps(item, ensure_ascii=False, indent=2))
    if len(items) > limit:
        print(f"... {len(items) - limit} éléments supplémentaires ...")

# ------------------- Génération -------------------
def generate_categories(count: int) -> List[Dict[str, Any]]:
    print(f"Génération de {count} catégories...")
    prompt = f"Génère {count} catégories de produits marocains. Réponds uniquement en JSON avec name et url."
    system_prompt = "Tu es un expert e-commerce. Réponds STRICTEMENT en JSON."
    parsed = request_llm_json(prompt, system_prompt)
    if parsed:
        categories = [{"name": c.get("name"), "url": c.get("url") or slugify(c.get("name"))} for c in parsed]
        if len(categories) < count:
            for i in range(len(categories), count):
                categories.append({"name": f"Catégorie {i+1}", "url": f"categorie-{i+1}"})
        return categories
    return [{"name": f"Catégorie {i+1}", "url": f"categorie-{i+1}"} for i in range(count)]

def generate_products(category_id: int, category_name: str, count: int) -> List[Dict[str, Any]]:
    products = []
    remaining = count
    while remaining > 0:
        batch = min(BATCH_SIZE_PRODUCTS, remaining)
        prompt = f"Génère {batch} produits marocains pour la catégorie {category_name}. JSON avec name, price, stock."
        system_prompt = "Tu es un expert produits alimentaires marocains. Réponds STRICTEMENT en JSON."
        parsed = request_llm_json(prompt, system_prompt)
        if parsed:
            for p in parsed:
                price = round(float(p.get("price", random.uniform(5, 200))), 2)
                stock = int(p.get("stock", random.randint(10, 500)))
                products.append({
                    "id": len(products)+1,
                    "name": p.get("name"),
                    "price": price,
                    "stock": stock,
                    "unit": random.choice(["KG","L","PIECE","G","UNIT"]),
                    "category_id": category_id,
                    "sku": f"SKU-{category_id:03d}-{len(products)+1:04d}",
                    "slug": slugify(p.get("name"))
                })
        else:
            for i in range(batch):
                idx = len(products)+1
                products.append({
                    "id": idx,
                    "name": f"Produit {category_name} {idx}",
                    "price": round(random.uniform(5,200),2),
                    "stock": random.randint(10,500),
                    "unit": random.choice(["KG","L","PIECE","G","UNIT"]),
                    "category_id": category_id,
                    "sku": f"SKU-{category_id:03d}-{idx:04d}",
                    "slug": slugify(f"{category_name}-{idx}")
                })
        remaining -= batch
    return products

def generate_users(count: int) -> List[Dict[str, Any]]:
    users = []
    remaining = count
    while remaining > 0:
        batch = min(BATCH_SIZE_USERS, remaining)
        prompt = f"Génère {batch} utilisateurs marocains réalistes. JSON avec firstName, lastName, email."
        system_prompt = "Tu es un expert profils clients marocains. Réponds STRICTEMENT en JSON."
        parsed = request_llm_json(prompt, system_prompt)
        if parsed:
            for p in parsed:
                idx = len(users)+1
                email = p.get("email") or f"{p.get('firstName','user')}.{p.get('lastName','user')}{idx}@{fake.domain_name()}"
                users.append({
                    "id": idx,
                    "firstName": p.get("firstName"),
                    "lastName": p.get("lastName"),
                    "email": email,
                    "phone": f"+212{random.randint(600000000,699999999)}",
                    "password": "$2a$10$dummy.hash.for.password",
                    "roles": [{"id":2,"label":"USER"}]
                })
        else:
            first_names = ["Ahmed","Fatima","Youssef","Aicha"]
            last_names = ["Alaoui","Benali","Idrissi"]
            for i in range(batch):
                idx = len(users)+1
                first = random.choice(first_names)
                last = random.choice(last_names)
                email = f"{first.lower()}.{last.lower()}{idx}@{fake.domain_name()}"
                users.append({
                    "id": idx,
                    "firstName": first,
                    "lastName": last,
                    "email": email,
                    "phone": f"+212{random.randint(600000000,699999999)}",
                    "password": "$2a$10$dummy.hash.for.password",
                    "roles": [{"id":2,"label":"USER"}]
                })
        remaining -= batch
    return users

def generate_orders(users: List[Dict[str,Any]], products: List[Dict[str,Any]], count:int) -> List[Dict[str,Any]]:
    orders = []
    statuses = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    for i in range(count):
        user = random.choice(users)
        items = []
        for product in random.sample(products, min(random.randint(1,5), len(products))):
            qty = random.randint(1,5)
            price = float(product["price"])
            items.append({"product_id": product["id"], "quantity": qty, "price": round(price,2)})
        subtotal = sum([it["quantity"]*it["price"] for it in items])
        total = round(subtotal,2)
        orders.append({
            "id": i+1,
            "user_id": user["id"],
            "items": items,
            "subTotal": subtotal,
            "totalAmount": total,
            "status": random.choice(statuses),
            "created_at": (datetime.now()-timedelta(days=random.randint(0,180))).isoformat()
        })
    return orders

# ------------------- Main -------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full","sample"], default="full")
    args = parser.parse_args()
    config = SAMPLE_CONFIG if args.mode=="sample" else FULL_CONFIG

    print(f"🚀 Génération données (mode {args.mode})")
    data: Dict[str,Any] = {}

    # Categories
    categories = generate_categories(config.categories)
    for idx, c in enumerate(categories,1):
        c["id"] = idx
    data["categories"] = categories
    preview("Catégories", categories)

    # Products
    products = []
    for c in categories:
        prod_count = random.randint(config.products_min, config.products_max)
        cat_products = generate_products(c["id"], c["name"], prod_count)
        products.extend(cat_products)
    data["products"] = products
    preview("Produits", products)

    # Users
    users = generate_users(config.users)
    data["users"] = users
    preview("Utilisateurs", users)

    # Orders
    orders = generate_orders(users, products, config.orders)
    data["orders"] = orders
    preview("Commandes", orders)

    # Save
    with open("synthetic_data.json","w",encoding="utf-8") as f:
        json.dump(data,f,ensure_ascii=False,indent=2)
    print(f"\n✅ Données sauvegardées dans synthetic_data.json")

if __name__=="__main__":
    main()
