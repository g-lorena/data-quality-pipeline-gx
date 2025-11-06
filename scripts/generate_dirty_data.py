import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_dirty_inventory_data(n_rows=100, scenario="mixed"):
    data = []
    
    for i in range(n_rows):
        row = {
            'product_id': f'PROD_{i+1:03d}',
            'product_name': f'Product {i+1}',
            'price': round(random.uniform(10, 1000), 2),
            'stock_quantity': random.randint(0, 500),
            'category': random.choice(['Electronics', 'Clothing', 'Food', 'Books']),
            'last_updated': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'supplier_email': f'supplier{i+1}@example.com'
        }

        # Injecter des erreurs selon le scénario
        if scenario in ["mixed", "schema_errors"]:
            if i % 15 == 0:  # ~7% des lignes
                row['product_id'] = f'INVALID_{i}'  # Format invalide
            if i % 20 == 0:  # ~5% des lignes
                row['product_name'] = None  # Valeur manquante
            if i % 18 == 0:
                row['last_updated'] = 'invalid-date'  # Date invalide
        
        if scenario in ["mixed", "business_errors"]:
            if i % 12 == 0:  # ~8% des lignes
                row['price'] = -round(random.uniform(10, 100), 2)  # Prix négatif
            if i % 10 == 0:  # 10% des lignes
                row['stock_quantity'] = -random.randint(1, 50)  # Stock négatif
            if i % 25 == 0:
                row['price'] = None  # Prix manquant
        
        if scenario in ["mixed", "quality_errors"]:
            if i % 30 == 0:  # ~3% des lignes
                row['supplier_email'] = 'invalid-email'  # Email invalide
            if i % 35 == 0:
                row['supplier_email'] = ''  # Email vide
            if i % 8 == 0 and i > 0:  # Créer des doublons
                row = data[0].copy()  # Duplique la première ligne
        
        if scenario == "clean":
            # S'assurer que tout est propre
            pass  # Pas de modifications
        
        data.append(row)
    
    df = pd.DataFrame(data)
    
    # Ajouter une colonne inattendue pour certains scénarios
    if scenario in ["mixed", "schema_errors"]:
        df['unexpected_column'] = 'surprise!'
    
    return df

def save_datasets():
    os.makedirs('data/raw', exist_ok=True)
    df_dirty = generate_dirty_inventory_data(n_rows=100, scenario="mixed")
    df_dirty.to_csv('data/raw/inventory_dirty.csv', index=False)