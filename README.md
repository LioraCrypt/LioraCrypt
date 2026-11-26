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

