from prometheus_client import Counter, Histogram, Gauge

# 1. Monitoring Ingestion : Pour suivre le remplissage des 35Go
# On utilise des labels pour différencier les tables (static, dynamic, live)
DB_RECORDS_PROCESSED = Counter(
    'db_records_total', 
    'Nombre de lignes traitees par l API', 
    ['table_name']
)

# 2. Performance de l'API : Temps de reponse des endpoints lourds (ex: merged)
API_RESPONSE_TIME = Histogram(
    'api_processing_seconds', 
    'Temps de traitement interne des requetes',
    buckets = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)

# 3. Santé de la connexion DB (0 ou 1)
# Utile pour savoir instantanément si l'API a perdu le lien avec Postgres
DATABASE_STATUS = Gauge('api_database_connected', 'Statut de la connexion a la base de données')

# 4. Monitoring du cache (si tu utilises Redis ou un cache local)
# Pour voir si tes requêtes tapent dans la DB ou dans le cache
CACHE_HIT_RATE = Counter('api_cache_hits_total', 'Nombre de requêtes servies par le cache', ['cache_type'])

# 5. Taille des fichiers de logs ou fichiers temporaires
# Très utile si Airflow génère beaucoup de logs sur ton volume Terraform
DISK_CLEANUP_NEEDED = Gauge('api_temporary_files_count', 'Nombre de fichiers temporaires en attente')