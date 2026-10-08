# HealthStream ICU Monitor - Séance 1 : Conteneurs et Bus de Données (Docker & Kafka)

Ce projet constitue la première brique de l'infrastructure **HealthStream ICU Monitor**, un système de supervision en temps réel pour un service de réanimation (ICU) de 12 lits. 

Il met en place un conteneur d'émulation de signes vitaux en Python, publiant des données physiologiques au format JSON dans un bus de messages **Apache Kafka** déployé en mode KRaft.

---

## Architecture du Projet

```text
s1-vitals-stream/
├── docker-compose.yml       # Orchestration des services Kafka et Producer
├── MESURES.md               # Relevés de mesures, configurations et justifications
├── README.md                # Documentation du projet
└── producer/
    ├── Dockerfile           # Image conteneurisée du producteur (non-root, cache optimisé)
    ├── .dockerignore        # Exclusion du contexte de build
    ├── requirements.txt     # Dépendances Python (confluent-kafka)
    └── producer.py          # Simulateur et producteur de signes vitaux MIMIC-IV
```

---

## Stack Technique

- **Moteur de conteneurisation :** Docker Engine (`29.8.2`)
- **Orchestration :** Docker Compose (`5.5.1`)
- **Bus de données :** Apache Kafka (`cp-kafka:7.8.0`) en mode **KRaft**
- **Langage / Runtime :** Python (`3.12.4`)
- **Bibliothèque Kafka :** `confluent-kafka` (`2.6.1`)

---

## Démarrage Rapide

### 1. Prérequis
S'assurer que Docker et Docker Compose v2 sont installés et fonctionnels sur votre poste.

```bash
docker --version
docker compose version
```

### 2. Lancement des Services
Pour construire les images, créer le réseau applicatif et démarrer l'ensemble des services en arrière-plan :

```bash
docker compose up -d --build
```

### 3. Vérification des Services et de la Santé (`Healthcheck`)
Pour contrôler l'état d'exécution des conteneurs :

```bash
docker compose ps
```

Le service `producer` attend que le service `kafka` passe à l'état `healthy` avant de s'exécuter.

### 4. Inspection des Logs du Producteur
Pour vérifier l'émission en continu des signes vitaux :

```bash
docker compose logs -f producer
```

---

## Structure des Messages Kafka

Les messages sont envoyés sur le topic `mimic-vitals`.

### Format de la Clé et du Payload (JSON)
- **Clé de partitionnement :** `patient_id` (ex: `P001`), encodé en UTF-8.
- **Payload :**

```json
{
  "patient_id": "P001",
  "event_time": 1773012000000,
  "vital_type": "HR",
  "value": 88.5,
  "unit": "bpm"
}
```

- **Garantie d'ordre :** Grâce au hachage de la clé (`murmur2_random`), tous les messages associés à un même patient sont systématiquement routés vers la même partition, garantissant un ordre chronologique strict.

---

## Configuration du Producteur Kafka

Le producteur intègre des mécanismes avancés de tolérance aux pannes et de résilience :

| Paramètre | Valeur | Description |
| --- | --- | --- |
| `acks` | `all` | Attente de l'acquittement du leader et de tous les réplicats ISR |
| `retries` | `5` | Tentatives de réémissions en cas d'erreur transitoire |
| `enable.idempotence` | `True` | Empêche la création de doublons ou l'inversion d'ordre lors des réémissions |
| `partitioner` | `murmur2_random` | Assure une compatibilité parfaite avec les consommateurs Java / Flink |
| `linger.ms` | `10` | Regroupement en lots pour optimiser le débit |

---

## Validation et Consommation manuelle

### Consulter les métadonnées du Topic Kafka
```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic mimic-vitals
```

### Lire les messages du flux en direct
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mimic-vitals \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true
```

---

## Nettoyage de l'Environnement

### Arrêt standard (Conservation des volumes de données) :
```bash
docker compose down
```

### Arrêt complet avec destruction des volumes (Kafka Data & Logs) :
```bash
docker compose down -v
```
