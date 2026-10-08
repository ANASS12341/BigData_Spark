# BigData_Spark
# Customer & Orders Analysis with PySpark

Projet de data processing avec **PySpark** : à partir de deux jeux de données (clients et commandes), le pipeline nettoie, joint et analyse les données pour en extraire des indicateurs métier (insights), puis sauvegarde les résultats au format **Parquet**.

## Objectifs

- Charger et explorer des données clients et commandes
- Nettoyer et préparer les données (nulls, doublons, types)
- Joindre les deux tables sans ambiguïté de colonnes
- Calculer des indicateurs avec `groupBy().agg()` et les **window functions**
- Sauvegarder les résultats en **Parquet**, partitionnés pour de meilleures performances

## Stack technique

- Python 3.x
- Apache Spark / PySpark
- Docker (image `jupyter/pyspark-notebook`)
- Jupyter Notebook
- Parquet

## Structure du projet

```
.
├── data/
│   ├── customers.csv
│   └── orders.csv
├── notebooks/
│   └── customers_orders_analysis.ipynb
├── output/
│   └── (résultats en Parquet)
├── docker-compose.yml
└── README.md
```

> Adapte cette arborescence à celle de ton repo.

## Description des données

**customers**

| Colonne | Description |
|---|---|
| customer_id | Identifiant du client |
| name | Nom du client |
| city / state / country | Localisation |
| registration_date | Date d'inscription |
| is_active | Client actif ou non |

**orders**

| Colonne | Description |
|---|---|
| order_id | Identifiant de la commande |
| customer_id | Identifiant du client (clé de jointure) |
| order_date | Date de la commande |
| total_amount | Montant de la commande |
| status | Statut (Shipped, Delivered, Cancelled...) |

## Installation et lancement

### Avec Docker

```bash
git clone https://github.com/<ton-utilisateur>/<ton-repo>.git
cd <ton-repo>

docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  -v "$(pwd)":/home/jovyan/work \
  jupyter/pyspark-notebook
```

Ouvre ensuite l'URL affichée dans le terminal (`http://127.0.0.1:8888/lab?token=...`), puis lance le notebook dans le dossier `work/notebooks/`.

La Spark UI est disponible sur `http://localhost:4040` pendant l'exécution.

### Sans Docker

```bash
python -m venv venv
source venv/bin/activate
pip install pyspark jupyter
```

Java 17 doit être installé.

## Pipeline de traitement

1. **Chargement** : lecture des CSV avec `spark.read.csv(..., header=True, inferSchema=True)`
2. **Exploration** : `printSchema()`, `describe()`, comptage des valeurs nulles
3. **Nettoyage** : `dropDuplicates()`, `dropna()` / `fillna()`, conversion des types de date
4. **Jointure** : `customers_df.join(orders_df, on="customer_id", how="inner")`
5. **Analyse** : agrégations et window functions
6. **Sauvegarde** : écriture en Parquet

## Insights extraits

- Nombre de commandes par client
- Montant total dépensé par client
- Panier moyen par statut de commande
- Répartition des commandes par ville / état
- Dernière commande de chaque client (window function `row_number`)
- Cumul des dépenses par client dans le temps
- Écart de montant entre commandes successives (`lag`)
- Taux d'annulation des commandes

## Exemples de code

**Total de commandes et montant par client**

```python
from pyspark.sql import functions as F

(
    customers_orders_df
    .groupBy("customer_id", "name")
    .agg(
        F.count("order_id").alias("nb_commandes"),
        F.round(F.sum("total_amount"), 2).alias("montant_total")
    )
    .orderBy(F.desc("montant_total"))
)
```

**Dernière commande de chaque client**

```python
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.desc("order_date"))

(
    customers_orders_df
    .withColumn("rang", F.row_number().over(w))
    .filter(F.col("rang") == 1)
    .drop("rang")
)
```

**Sauvegarde en Parquet**

```python
result_df.write.mode("overwrite").parquet("output/customer_summary")
```

## Concepts Spark abordés

- Transformations vs actions (évaluation paresseuse)
- Jointures et gestion des colonnes ambiguës
- Agrégations avec `groupBy().agg()`
- Window functions : `row_number`, `rank`, `lag`, somme cumulée
- Shuffle et partitionnement
- Format Parquet (stockage en colonnes, compression, column pruning)

## Résultats

Ajoute ici des captures d'écran ou des extraits de tableaux de résultats (top clients, répartition par ville, etc.).

## Pistes d'amélioration

- Ajouter une source de données via une API (par exemple taux de change)
- Partitionner les sorties Parquet par date ou par pays
- Utiliser `broadcast()` pour la jointure si une table est petite
- Ajouter des tests de qualité de données
- Orchestrer le pipeline avec Airflow

## Auteur

**OUCHTOUBANE Anass**
[LinkedIn](https://www.linkedin.com/in/anass-ouchtoubane-910b192a6/?isSelfProfile=true) · [GitHub](https://github.com/ANASS12341)
