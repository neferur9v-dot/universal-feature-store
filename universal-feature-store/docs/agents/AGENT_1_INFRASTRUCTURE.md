# Задание для Агента 1: Infrastructure & DevOps Engineer

## 🎯 Роль
Вы отвечаете за всю инфраструктуру проекта: Docker Compose конфигурацию, сети, тома, инициализацию сервисов (Kafka, Redis, MinIO, Feast, MLflow). Ваша работа — фундамент, на котором строятся все остальные компоненты.

## 📂 Расположение файлов
Все ваши файлы должны находиться в папке: `/workspace/ship-feature-store/infra/`

## 📋 Список задач

### 1. Docker Compose конфигурация
Создайте файл `infra/docker-compose.yml`, который описывает **13 сервисов**:

#### Уровень Инфраструктуры:
- **zookeeper** (image: confluentinc/cp-zookeeper:7.5.0)
  - Ports: 2181:2181
  - Env: ZOOKEEPER_CLIENT_PORT=2181
  
- **kafka** (image: confluentinc/cp-kafka:7.5.0)
  - Ports: 9092:9092, 29092:29092 (internal)
  - Env: KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181, KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
  - Depends on: zookeeper

- **redis** (image: redis:7-alpine)
  - Ports: 6379:6379
  - Command: redis-server --appendonly yes

- **minio** (image: minio/minio:latest)
  - Ports: 9000:9000, 9001:9001
  - Env: MINIO_ROOT_USER=minioadmin, MINIO_ROOT_PASSWORD=minioadmin
  - Command: server /data --console-address ":9001"

- **mlflow** (image: ghcr.io/mlflow/mlflow:latest)
  - Ports: 5000:5000
  - Env: MLFLOW_S3_URI=s3://mlflow-artifacts, AWS_ACCESS_KEY_ID=minioadmin, AWS_SECRET_ACCESS_KEY=minioadmin, MLFLOW_S3_ENDPOINT_URL=http://minio:9000
  - Command: mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri sqlite:///mlflow.db --default-artifact-root s3://mlflow-artifacts/

#### Уровень Обработки:
- **flink-jobmanager** (image: flink:1.17-scala_2.12-python3.9)
  - Ports: 8081:8081
  - Command: jobmanager
  - Env: FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager

- **flink-taskmanager** (image: flink:1.17-scala_2.12-python3.9)
  - Depends on: flink-jobmanager, kafka, redis, minio
  - Command: taskmanager
  - Env: FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager, taskmanager.numberOfTaskSlots: 4
  - Volumes: ./streaming:/opt/flink/usrlib (для доступа к Flink job)

- **feast-online-server** (image: feastdev/feast-online-server:latest)
  - Ports: 6566:6566
  - Env: FEAST_CONFIG=/etc/feast/feast.yaml
  - Volumes: ./feature_repo:/etc/feast

#### Уровень Приложений:
- **api-server** (build: ./api)
  - Ports: 8000:8000
  - Env: MLFLOW_TRACKING_URI=http://mlflow:5000, REDIS_HOST=redis, KAFKA_BROKER=kafka:29092, FEAST_SERVING_URL=feast-online-server:6566
  - Depends on: mlflow, redis, kafka, feast-online-server

- **trainer** (build: ./ml)
  - Env: MLFLOW_TRACKING_URI=http://mlflow:5000, MINIO_ENDPOINT=minio:9000, ICEBERG_WAREHOUSE=s3://iceberg-data/warehouse
  - Depends on: minio, mlflow

- **drift-detector** (build: ./ml)
  - Env: MLFLOW_TRACKING_URI=http://mlflow:5000, MINIO_ENDPOINT=minio:9000
  - Depends on: minio, mlflow

#### Уровень Симуляции:
- **cpp-telemetry-daemon** (build: ./simulators/cpp_qt5)
  - Env: KAFKA_BROKER=kafka:29092
  - Depends on: kafka

- **init-job** (image: python:3.10-slim)
  - Command: python /app/init.py (создает топики Kafka, применяет Feast config, загружает демо-модели)
  - Volumes: ./infra/init:/app
  - Depends on: kafka, minio, mlflow, feast-online-server

#### Уровень Мониторинга:
- **prometheus** (image: prom/prometheus:latest)
  - Ports: 9090:9090
  - Volumes: ./infra/prometheus.yml:/etc/prometheus/prometheus.yml

- **grafana** (image: grafana/grafana:latest)
  - Ports: 3000:3000
  - Env: GF_SECURITY_ADMIN_PASSWORD=admin
  - Volumes: ./infra/grafana-datasources.yml:/etc/grafana/provisioning/datasources/datasources.yml

### 2. Сети и тома
Создайте в `docker-compose.yml`:
- Network: `ship-network` (driver: bridge)
- Volumes:
  - `kafka-data`
  - `redis-data`
  - `minio-data`
  - `mlflow-data`
  - `iceberg-data`
  - `flink-checkpoints`

### 3. Конфигурационные файлы
Создайте следующие файлы в папке `infra/`:

#### `infra/kafka-topics.json`
```json
{
  "topics": [
    {"name": "vessel_events", "partitions": 3, "replication_factor": 1},
    {"name": "inference_logs", "partitions": 3, "replication_factor": 1}
  ]
}
```

#### `infra/feast_config.yaml`
Опишите Feature Store конфигурацию:
```yaml
project: ship_features
registry: s3://iceberg-data/feast/registry.db
online_store:
  type: redis
  connection_string: redis:6379
offline_store:
  type: iceberg
  path: s3://iceberg-data/warehouse
  warehouse: s3://iceberg-data/warehouse
```

#### `infra/init.py`
Python скрипт для инициализации:
- Создает топики Kafka через `kafka.admin.AdminClient`
- Выполняет `feast apply` для применения конфига
- Загружает демо-модели в MLflow (простые sklearn модели для теста)

#### `infra/prometheus.yml`
Конфигурация Prometheus для сканирования метрик со всех сервисов.

#### `infra/grafana-datasources.yml`
Автоматическое добавление Prometheus как источника данных в Grafana.

### 4. Dockerfile для сервисов
Создайте базовые Dockerfile в соответствующих папках, если они требуют кастомной сборки:
- `api/Dockerfile`
- `ml/Dockerfile` (для trainer и drift-detector)
- `simulators/cpp_qt5/Dockerfile` (для C++ демона)

## 🔗 Точки интеграции
- **Kafka Broker**: `kafka:29092` (internal), `localhost:9092` (external)
- **Redis**: `redis:6379`
- **MinIO**: `http://minio:9000`, credentials: minioadmin/minioadmin
- **MLflow**: `http://mlflow:5000`
- **Feast Serving**: `feast-online-server:6566`
- **Flink JobManager**: `http://flink-jobmanager:8081`

## ✅ Критерии приемки
1. Все 13 сервисов запускаются командой `docker-compose up --build` без ошибок.
2. Сервисы видят друг друга по именам внутри сети `ship-network`.
3. Kafka топики `vessel_events` и `inference_logs` создаются автоматически при старте.
4. MinIO bucket `iceberg-data` создан и доступен.
5. MLflow сервер запущен и принимает запросы на регистрацию моделей.
6. Feast конфигурация применена и онлайн-сервер отвечает на gRPC запросы.
7. Init-job завершается успешно после инициализации.

## 🛠️ Технические требования
- Используйте Docker Compose version 3.8+
- Все образы должны быть с конкретными версиями (не `latest`, где критично)
- Предусмотрите health checks для критических сервисов (Kafka, Redis, MLflow)
- Логи должны выводиться в stdout для отладки через `docker-compose logs`

## 📝 Примечания
- Убедитесь, что пути к томам корректны относительно корня проекта
- Для Flink укажите правильную версию с поддержкой Python (PyFlink)
- Предусмотрите возможность масштабирования taskmanager'ов в будущем
