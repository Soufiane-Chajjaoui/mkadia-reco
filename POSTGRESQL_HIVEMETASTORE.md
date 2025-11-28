# Configuration PostgreSQL + HiveMetaStore

## Aperçu

Cette architecture utilise **PostgreSQL** comme backend pour **HiveMetaStore** au lieu de MySQL. Les catalogues de métadonnées sont maintenant stockés dans PostgreSQL, ce qui offre :

- ✅ **Réutilisation** : PostgreSQL existant (postgres-reco) est utilisé
- ✅ **Unification** : Un seul système de base de données pour les données et métadonnées
- ✅ **Performance** : PostgreSQL optimisé pour les requêtes métadonnées
- ✅ **Compatibilité** : Support natif de HiveMetaStore

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
    │  - mkadia-db    │  │  (Data Lake)     │
    │  - metastore    │  │  - Parquet files │
    └─────────────────┘  └──────────────────┘
           │ (Thrift connection)
           │
       ┌───┴──────────────┐
       │ HiveMetaStore    │
       │ (port 9083)      │
       └──────────────────┘
```

## Modifications apportées

### 1. docker-compose.yml

- **Suppression** : Service MySQL
- **PostgreSQL** : Configuration pour HiveMetaStore (nouvelle base de données `metastore`)
- **HiveMetaStore** : Configuré pour utiliser PostgreSQL comme backend

```yaml
postgres:
  volumes:
    - ./hive-init-postgres.sql:/docker-entrypoint-initdb.d/01-hive-init.sql

hive-metastore:
  environment:
    DB_DRIVER: org.postgresql.Driver
    DB_HOST: postgres-reco
    DB_PORT: 5432
    DB_NAME: metastore
```

### 2. spark/Dockerfile

- **Changement** : Driver MySQL → Driver PostgreSQL
- **JAR** : `postgresql-42.6.0.jar`

```dockerfile
curl -fsSL -o /opt/spark/jars/extra/postgresql-42.6.0.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
```

### 3. hive-init-postgres.sql

Script d'initialisation pour créer la base de données `metastore` dans PostgreSQL lors du démarrage.

## Base de données PostgreSQL

### Structure

**PostgreSQL** gère désormais **deux** bases de données :

| Base de données | Usage | Tables |
|---|---|---|
| **mkadia-db** | Données métier (recommendations) | recommendations |
| **metastore** | Métadonnées Hive | tbls, sds, columns_v2, dbs, funcs, partitions, etc. |

### Schéma HiveMetaStore

Les tables principales dans la base `metastore` :

```
DBS                    -- Bases de données Hive
├─ TBLS                -- Tables
│  ├─ SDS              -- Storage descriptors
│  │  ├─ COLUMNS_V2    -- Colonnes
│  │  └─ SKEWED_COL_NAMES
│  ├─ PARTITIONS       -- Partitions
│  └─ PARTITION_KEYS
├─ FUNCS               -- Fonctions UDF
└─ SKEWED_STRINGCOL
```

## Utilisation

### Accéder à PostgreSQL

#### Depuis l'hôte
```bash
psql -h localhost -p 5433 -U postgres -d mkadia-db
```

#### Dans PostgreSQL, voir les bases
```sql
\l              -- Lister les bases
\c metastore    -- Basculer vers base metastore
\dt             -- Lister les tables
```

#### Requêter les métadonnées HiveMetaStore

Voir toutes les tables Hive créées :
```sql
SELECT t.tbl_name, db.name as db_name, s.location
FROM tbls t
JOIN dbs db ON t.db_id = db.db_id
LEFT JOIN sds s ON t.sd_id = s.sd_id;
```

Voir les colonnes d'une table :
```sql
SELECT c.column_name, c.type_name
FROM columns_v2 c
JOIN sds s ON c.cd_id = s.cd_id
JOIN tbls t ON s.sd_id = t.sd_id
WHERE t.tbl_name = 'recommendations_hive';
```

### Accéder aux données via Spark

Les recommandations sont toujours stockées en Parquet dans MinIO et cataloguées dans HiveMetaStore :

```python
# Lire depuis Hive
df = spark.sql("SELECT * FROM recommendations_hive WHERE user_id = 123")

# Écrire vers Hive
df.write.insertInto("recommendations_hive")
```

## Flux de données

1. **Kafka** → Events (review, favorite, cart, order)
2. **Spark Processing** → Calcul recommendations (ALS model)
3. **MinIO** → Stockage Parquet des recommandations
4. **HiveMetaStore** → Catalogue des métadonnées (PostgreSQL)
5. **PostgreSQL** → Métadonnées structurées

## Déploiement

### Démarrage

```bash
docker-compose up -d

# Vérifier les services
docker-compose ps

# Vérifier les logs
docker-compose logs hive-metastore
```

### Vérifications

PostgreSQL + HiveMetaStore prêts :
```bash
# PostgreSQL
docker exec postgres-reco psql -U postgres -d metastore -c "SELECT COUNT(*) FROM information_schema.tables;"

# HiveMetaStore
docker exec hive-metastore schematool -dbType postgres -info
```

### Initialisation du schéma Hive

Si le schéma n'est pas initialisé automatiquement :
```bash
docker exec hive-metastore schematool -dbType postgres -initSchema
```

## Performance et configuration

### PostgreSQL optimisée pour HiveMetaStore

Configuration recommandée dans PostgreSQL :
- `shared_buffers = 256MB` (pour métadonnées)
- `work_mem = 64MB`
- `maintenance_work_mem = 128MB`

### MinIO + Parquet

- Format : Parquet avec compression Snappy
- Localisation : `s3a://mkadia-warehouse/recommendations/`
- Avantages : 80-90% d'espace économisé vs JSON

## Monitoring

### PostgreSQL (Métastore)

```bash
psql -h localhost -p 5433 -U postgres -d metastore -c "
  SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
  FROM pg_tables
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"
```

### HiveMetaStore (Thrift)

Port : `9083` (accessible depuis Spark)

### MinIO

URL : http://localhost:9021  
Credentials : minioadmin / minioadmin

## Troubleshooting

### PostgreSQL ne démarre pas
```bash
docker-compose logs postgres
```

### HiveMetaStore ne se connecte pas à PostgreSQL
```bash
docker exec hive-metastore nc -zv postgres-reco 5432
```

### Base de données metastore n'existe pas
```bash
docker exec postgres-reco psql -U postgres -c "CREATE DATABASE metastore;"
```

### Schéma Hive non créé
```bash
docker exec hive-metastore schematool -dbType postgres -initSchema 2>&1 | tail -20
```

## Fichiers modifiés

- ✅ `docker-compose.yml` → PostgreSQL pour HiveMetaStore
- ✅ `spark/Dockerfile` → Driver PostgreSQL
- ✅ `spark/apps/streaming.py` → Support Parquet + Hive
- ✅ `hive-init-postgres.sql` → Script d'initialisation
- ✅ `HIVEMETASTORE_SETUP.md` → Documentation mise à jour
- ✅ `POSTGRESQL_HIVEMETASTORE.md` → Ce fichier
