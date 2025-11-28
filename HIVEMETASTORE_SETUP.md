# Architecture MinIO + HiveMetaStore + Parquet

## Vue d'ensemble

L'architecture a été mise à jour pour utiliser :
- **MinIO** : Data Lake (stockage objet S3-compatible)
- **HiveMetaStore** : Catalogue de métadonnées
- **Parquet** : Format de stockage optimisé pour Spark/Hive

## Architecture

```
┌─────────────────────────────────────────────────────┐
│          Spark Streaming Application                │
│   (Kafka → Processing → Recommendations)            │
└─────────────────────────────────────────────────────┘
           │                    │
           ▼                    ▼
    ┌─────────────────┐  ┌──────────────────┐
    │   PostgreSQL    │  │   MinIO (S3)     │
    │  (metadata DB)  │  │  (Data Lake)     │
    └─────────────────┘  └──────────────────┘
           ▲                    ▲
           │                    │
           │ ┌──────────────────┘
           │ │
       ┌───┴───────────────┐
       │ HiveMetaStore     │
       │ + MySQL Backend   │
       └───────────────────┘
```

## Services Docker

### PostgreSQL (Backend HiveMetaStore)
- **Container** : `postgres-reco`
- **Port** : 5433 (externe) / 5432 (interne)
- **Database** : metastore (créée automatiquement)
- **Credentials** : postgres / soufianch

### Hive MetaStore
- **Container** : `hive-metastore`
- **Port** : 9083 (Thrift)
- **Backend DB** : PostgreSQL
- **Location** : `/user/hive/warehouse` (s3a://mkadia-warehouse/hive-warehouse)

### MinIO (Data Lake)
- **Container** : `minio-reco`
- **Port API** : 9020
- **Port Console** : 9021
- **Bucket** : mkadia-warehouse
- **Credentials** : minioadmin / minioadmin

## Configuration Spark

La session Spark est configurée avec :

```python
spark = (
    SparkSession.builder
    .enableHiveSupport()  # Support Hive
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-reco:9000")
    .config("spark.sql.warehouse.dir", "s3a://mkadia-warehouse/hive-warehouse")
    # ... autres configurations S3
    .getOrCreate()
)
```

## Stockage des données

### Format Parquet
Les recommandations sont stockées en format Parquet :
```
s3a://mkadia-warehouse/recommendations/batch_{batch_id}/user_{user_id}/
```

**Avantages du format Parquet** :
- ✅ Compression optimale (snappy par défaut)
- ✅ Schéma fortement typé
- ✅ Lecture columnar (plus rapide que JSON)
- ✅ Compatible avec Hive, Spark, Presto, etc.

### Table Hive

Créée automatiquement au démarrage :
```sql
CREATE TABLE IF NOT EXISTS recommendations_hive (
    user_id INT,
    item_id INT,
    score DOUBLE,
    rank INT,
    batch_id LONG,
    timestamp TIMESTAMP
)
USING PARQUET
LOCATION 's3a://mkadia-warehouse/recommendations/'
```

## Utilisation

### Accéder aux recommandations via Spark SQL

```python
spark.sql("SELECT * FROM recommendations_hive WHERE user_id = 123").show()
```

### Lister les tables Hive

```python
spark.sql("SHOW TABLES").show()
```

### Interroger les métadonnées

```python
spark.sql("DESCRIBE recommendations_hive").show()
```

### Exporter en Parquet

```python
df = spark.sql("SELECT * FROM recommendations_hive")
df.write.parquet("s3a://mkadia-warehouse/exports/export_batch_123/")
```

### Interroger les métadonnées depuis PostgreSQL

Connexion à PostgreSQL :
```bash
psql -h localhost -p 5433 -U postgres -d metastore
```

Lister les tables Hive stockées en PostgreSQL :
```sql
SELECT * FROM tbls;
SELECT * FROM sds;  -- Storage descriptors
SELECT * FROM columns_v2;  -- Colonnes des tables
```

## Monitoring

### MinIO Console
Accessible via : http://localhost:9021

### Spark UI
Accessible via : http://localhost:8081

### Fichier de sortie
Logs à checker dans les logs du conteneur spark-master

## Variables d'environnement

```bash
# MinIO (Data Lake)
S3_ENDPOINT=http://minio-reco:9000
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin

# HiveMetaStore
METASTORE_URI=thrift://hive-metastore:9083
DB_DRIVER=org.postgresql.Driver
DB_HOST=postgres-reco
DB_PORT=5432
DB_NAME=metastore
DB_USER=postgres
DB_PASSWD=soufianch
```

## Démarrage

```bash
docker-compose up -d

# Vérifier que tous les services sont healthy
docker-compose ps
```

## Schéma PostgreSQL pour HiveMetaStore

Les métadonnées sont stockées dans PostgreSQL avec les tables principales :

- **TBLS** : Définition des tables Hive
- **SDS** : Storage descriptors (emplacement, format, colonnes)
- **COLUMNS_V2** : Schéma des colonnes
- **PARTITIONS** : Partitions des tables
- **DBS** : Bases de données Hive
- **FUNCS** : Fonctions UDF

Exemple de requête pour voir les tables créées :
```sql
SELECT t.tbl_name, db.name as database, s.location
FROM tbls t
JOIN dbs db ON t.db_id = db.db_id
LEFT JOIN sds s ON t.sd_id = s.sd_id
WHERE t.tbl_name LIKE 'recommendations%';
```

## Troubleshooting

### HiveMetaStore ne démarre pas
Vérifier que PostgreSQL est healthy :
```bash
docker-compose logs postgres
```

### MinIO bucket non trouvé
Créer le bucket via la console MinIO ou :
```python
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio-reco:9000', ...)
s3.create_bucket(Bucket='mkadia-warehouse')
```

### Erreur de connexion Spark → HiveMetaStore
Vérifier que le host `hive-metastore` est résolvable depuis le conteneur Spark :
```bash
docker exec spark-master nslookup hive-metastore
```

### PostgreSQL metastore n'initialise pas les tables Hive
Vérifier que le schéma Hive a été créé dans PostgreSQL :
```bash
psql -h localhost -p 5433 -U postgres -d metastore -c "\dt" | grep -i hive
```

Si aucune table n'existe, initialiser manuellement :
```bash
docker exec hive-metastore schematool -dbType postgres -initSchema
```

## Performance

- **Compression** : Snappy (rapide)
- **Warehouse** : S3A (MinIO compatible)
- **Tables** : Parquet (columnar format)
- **Métastore** : MySQL (persistant)

Pour améliorer les performances :
1. Augmenter `HADOOP_HEAPSIZE` dans HiveMetaStore
2. Ajuster les paramètres Spark workers en fonction de vos ressources
3. Utiliser le partitioning pour les grandes tables
