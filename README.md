# LioraCrypt

Projet de fin de formation Data Engineer chez Liora.

## Description

Projet réalisé en équipe dans le cadre de notre formation Data Engineer.

## Organisation

Le projet est organisé avec GitHub Projects (Kanban) pour suivre l'avancement des tâches.

## Test webhook

Test de notification Discord avec Captain Hook.

## Déploiement et automatisation

Le pipeline LioraCrypt est conteneurisé avec Docker et déployé sur Google Cloud Platform (GCP).

### Pipeline

Le script `run.sh` orchestre les différentes étapes :

- Extraction des données depuis l'API Binance.
- Chargement des données brutes dans BigQuery.
- Transformation et validation des données avec dbt.

En cas d'erreur, le pipeline s'arrête automatiquement.

### Docker

Construction de l'image Docker :

```bash
docker build -t lioracrypt .
```

L'exécution locale nécessite une authentification Google Cloud ainsi qu'une configuration dbt adaptée.

### Infrastructure cloud

- **Artifact Registry** : stockage de l'image Docker.
- **Cloud Run Jobs** : exécution du pipeline.
- **Cloud Scheduler** : déclenchement automatique hebdomadaire.
- **Cloud Logging** : consultation des logs et suivi des erreurs.
- **BigQuery** : stockage et traitement des données.


## Exécution du pipeline avec Docker

### 1. Prérequis

- Docker Desktop
- Google Cloud CLI
- Accès au projet GCP `lioracrypt`

### 2. Authentification Google Cloud

```cmd
gcloud auth application-default login
```

### 3. Construction de l'image Docker

Depuis la racine du projet :

```cmd
docker build -t lioracrypt-test .
```

### 4. Exécution du conteneur (Windows CMD)

```cmd
docker run --rm -v "%APPDATA%\gcloud:/credentials:ro" -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/application_default_credentials.json lioracrypt-test
```

Le conteneur exécute automatiquement le script `run.sh` :

- Récupération des données Binance.
- Chargement des données dans BigQuery.
- Transformation et tests avec dbt.

Les identifiants Google Cloud sont montés en lecture seule et ne sont pas intégrés à l'image Docker.