# Airlines
```
docker exec -it dst_airlines-airflow-scheduler-1 airflow dags reserialize
docker compose run --rm airflow-cli bash
```

-- Optionnel : vide les positions live pour repartir sur un dashboard propre
TRUNCATE TABLE live_data;
-- Marque les vieux vols comme arrivés pour que le triage les ignore
UPDATE flight_dynamic SET status = 'arrived' WHERE status IN ('en route', 'departing');

docker compose exec airflow-scheduler airflow dags delete etl

```
docker compose exec postgres psql -U airflow -d airlines
select * from live_data;
# select count(distinct timestamp) from live_data;
```

```
COPY opensky_flights TO '/tmp/live_data.csv' DELIMITER ',' CSV HEADER;
```

```
>>> a = {item["callsign"] for item in live_rows}
>>> b = {item["callsign"] for item in static_rows}
>>> a-b # ou b-a
```

```
docker compose down --volumes --rmi all
docker compose up --build -d
```

```
http://localhost:8080/
http://localhost:8000/docs
```



Option 2 : Automatiser et surveiller (Le MLOps)C'est pour rendre ton projet plus professionnel. On peut :Déclenchement automatique : Configurer Airflow pour qu'il relance l'entraînement tous les lundis avec les nouvelles données de la semaine.Alerting : Si le score $R^2$ descend en dessous de 0.90 (le modèle devient moins bon), Airflow t'envoie une alerte.Grafana : Afficher tes métriques (MAE, RMSE) sur un beau tableau de bord au lieu de regarder des logs noirs et blancs.






1. Architecture de Collecte (Stack Technique)Pour que ce plan fonctionne, nous utiliserons la chaîne suivante :Exporters : Node Exporter (Machine), cAdvisor (Containers), Postgres Exporter (DB).Pushgateway : Pour recevoir les métriques "one-shot" d'Airflow (Scores, lignes d'entraînement).Prometheus : Centralisation de toutes les séries temporelles.MLflow : Stockage des artefacts visuels (Feature Importance, Résidus).Grafana : Dashboard unique mixant données Prometheus et Iframe/Images MLflow.
2. Le Dashboard "Command Center" (4 Colonnes)A. Santé de l'Hôte & Stabilité (Infrastructure)L'objectif est de vérifier que l'entraînement n'étouffe pas l'inférence.CPU & RAM : Monitoring des pics pendant les DAGs Airflow.Disk Pressure : Alerting critique sur /var/lib/docker/volumes (Postgres & MLflow).Docker Health : Surveillance des OOM Kills (Memory Usage vs Limit) pour l'API.B. Intégrité de la Donnée (Upstream)Avant de regarder le modèle, on regarde la matière première.Data Volume : ml_training_rows_count (Suivi de la croissance du dataset).ETL Health : Ratio Commits/Rollbacks sur Postgres.Feature Drift : Moyenne glissante des features clés (ex: retard moyen à l'arrivée) pour détecter un changement de comportement du monde réel.C. Le Duel : Champion vs Challenger (ML Performance)C'est le cœur de ton système de promotion.Métrique de Succès : Comparaison $R^2$ et $MAE$ (Dernier Run vs Production).Score Distribution : Un histogramme comparant les prédictions du modèle en prod et du nouveau modèle sur le même échantillon de test.Model Latency : Temps de chargement ($s$) et latence d'inférence (p95 en $ms$).D. Audit de Confiance (Explainability)Validation visuelle pour éviter les modèles "boîte noire" ou biaisés.Feature Importance : Affichage de l'artefact MLflow. Si une variable technique (id) ressort, on rejette.Analyse des Résidus : Vérifier si l'erreur est distribuée normalement ou si le modèle échoue systématiquement sur les retards extrêmes.
3. Logique de Promotion (Le "Gatekeeper")Le passage du Challenger au Champion ne doit se faire que si tous ces feux sont au vert :Technique : Le container de l'API accepte le nouveau modèle sans exploser la RAM.Statistique : $R^2_{Challenger} > R^2_{Champion}$ sur le dataset de test.Opérationnel : Latence d'inférence < 200ms.Éthique/Logique : La Feature Importance est cohérente avec les lois de l'aviation.