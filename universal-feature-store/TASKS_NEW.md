# 🚀 Ship Feature Store: Задачи для Агентов-Разработчиков

Этот документ содержит точные технические задания для 4-х специализированных агентов. 
Каждый агент работает в своей директории, соблюдая четкие интерфейсы интеграции.

**Общая цель:** Построить единую систему Feature Store для обработки телеметрии судна с использованием RNN/LSTM моделей.

---

## 🏗️ Структура проекта (Единое пространство)

Все агенты работают в рамках единого репозитория `/workspace/ship-feature-store/`:

```text
ship-feature-store/
├── infra/                  # [Агент 1] Инфраструктура и Docker
│   ├── docker-compose.yml
│   ├── .env
│   └── configs/
│       ├── kafka/
│       ├── flink/
│       └── feast/
│
├── streaming/              # [Агент 2] Потоковая обработка (Flink)
│   ├── src/
│   │   ├── flink_job.py        # Основной PyFlink Job
│   │   ├── schema.py           # Схемы данных Avro/JSON
│   │   └── utils/
│   ├── requirements.txt
│   └── Dockerfile
│
├── ml/                     # [Агент 3] ML Модели (LSTM/RNN)
│   ├── src/
│   │   ├── models/
│   │   │   ├── lstm.py         # Архитектуры LSTM/GRU
│   │   │   └── attention.py    # Attention механизмы
│   │   ├── training/
│   │   │   ├── train.py        # Скрипт обучения
│   │   │   └── dataset.py      # DataLoader для временных рядов
│   │   └── registry.py         # Работа с MLflow
│   ├── requirements.txt
│   └── Dockerfile
│
├── api/                    # [Агент 4] Backend сервис
│   ├── src/
│   │   ├── main.py           # FastAPI приложение
│   │   ├── services/
│   │   │   ├── predictor.py  # Логика инференса
│   │   │   └── feature_store.py # Клиент Feast/Redis
│   │   └── schemas.py        # Pydantic модели
│   ├── requirements.txt
│   └── Dockerfile
│
├── simulators/             # [Агент 4] C++ Симуляторы
│   └── cpp_qt5/
│       ├── src/
│       ├── include/
│       ├── CMakeLists.txt
│       └── Dockerfile
│
├── tests/                  # Общие тесты
└── docs/                   # Документация
```

---

## 👤 Агент 1: Infrastructure & DevOps Engineer

**Директория работы:** `infra/`  
**Ответственность:** Поднятие всего стека из 13 контейнеров, сеть, тома, конфигурация.

### ✅ Задачи:
1. **Docker Compose (`docker-compose.yml`)**:
   - Описать 13 сервисов: `zookeeper`, `kafka`, `redis`, `minio`, `mlflow`, `flink-jobmanager`, `flink-taskmanager`, `feast-online-server`, `api-server`, `trainer`, `drift-detector`, `cpp-telemetry-daemon`, `init-job`.
   - Настроить единую сеть `ship-network`.
   - Пробросить порты: Kafka (9092), Redis (6379), MinIO (9000/9001), MLflow (5000), Flink UI (8081), API (8000), Grafana (3000).

2. **Конфигурация сервисов**:
   - **Kafka**: Создать топик `vessel_events` и `inference_logs` при старте (через init-job или команду).
   - **MinIO**: Создать бакет `iceberg-data` и `mlflow-artifacts`.
   - **Feast**: Подготовить `feature_store.yaml` (указать Redis и S3/MinIO пути).

3. **Init Job**:
   - Написать скрипт `init.sh`, который ждет поднятия Kafka и Redis, затем применяет `feast apply` и создает топики.

### 🔗 Точки интеграции:
- Предоставить переменные окружения для других сервисов (например, `KAFKA_BROKER=kafka:9092`, `REDIS_HOST=redis`).
- Убедиться, что `flink-taskmanager` видит `jobmanager`.

### 📦 Результат:
- Файл `infra/docker-compose.yml` готов к запуску `docker-compose up`.
- Все сервисы стартуют без ошибок подключения.

---

## 👤 Агент 2: Big Data Streaming Engineer (Flink)

**Директория работы:** `streaming/`  
**Ответственность:** Обработка потока телеметрии, оконные агрегаты, запись в Online/Offline хранилища.

### ✅ Задачи:
1. **Схема данных (`schema.py`)**:
   - Определить структуру события: `entity_id`, `timestamp`, `temperature`, `rpm`, `speed`, `fuel`, `vibration`.

2. **Flink Job (`flink_job.py`)**:
   - **Source**: Чтение из Kafka топика `vessel_events`.
   - **Transformations**:
     - Парсинг JSON.
     - Вычисление агрегатов через Tumbling Event Time Windows (1 мин, 5 мин, 1 час).
     - Расчет `avg_temp`, `max_rpm`, `std_vibration`.
   - **Sinks**:
     - **Redis**: Запись последних агрегатов по ключу `fs:{entity_id}:agg`.
     - **Iceberg/MinIO**: Запись "сырых" и агрегированных данных партициями по часу/дням.

3. **Dockerfile**:
   - Базовый образ `flink:1.17-python`.
   - Установка зависимостей: `apache-flink`, `pymupdf`, `redis`, `pyiceberg`.

### 🔗 Точки интеграции:
- Входные данные: формат JSON от C++ демона.
- Выходные данные: ключи в Redis вида `fs:VSL-001:engine_temp_avg_5m`.
- Выходные данные: таблица Iceberg `vessel_telemetry_raw` в MinIO.

### 📦 Результат:
- Код PyFlink приложения, готовый к отправке на кластер.
- Данные появляются в Redis через 10-20 секунд после старта потока.

---

## 👤 Агент 3: ML Engineer (Time Series & RNN)

**Директория работы:** `ml/`  
**Ответственность:** Разработка LSTM/GRU моделей, пайплайны обучения, регистрация в MLflow.

### ✅ Задачи:
1. **Архитектура моделей (`models/lstm.py`)**:
   - Реализовать `LSTMModel` (PyTorch): 2 слоя LSTM, Dropout, Linear слой.
   - Реализовать `AttentionLSTM`: слой внимания поверх выходов LSTM.
   - Вход: последовательность длиной `seq_length=60` шагов, 8 признаков.
   - Выход: вероятность отказа (binary classification) или прогноз температуры (regression).

2. **Подготовка данных (`training/dataset.py`)**:
   - Функция `create_sequences(data, seq_length)`: нарезка временных рядов на окна.
   - Нормализация (MinMaxScaler или StandardScaler).
   - Разделение на Train/Val/Test (80/10/10).

3. **Обучение (`training/train.py`)**:
   - Чтение исторических данных из CSV/Parquet (симуляция Offline Store).
   - Цикл обучения с валидацией.
   - Логирование метрик (Loss, Accuracy, AUC) в MLflow.
   - Сохранение артефакта модели (`model.pth`) в MLflow Model Registry.

4. **Dockerfile**:
   - Образ `python:3.10`.
   - Зависимости: `torch`, `pandas`, `scikit-learn`, `mlflow`, `boto3` (для MinIO).

### 🔗 Точки интеграции:
- Модель регистрируется в MLflow с именем `lstm_engine_failure`.
- Версия модели берется из env переменной `MODEL_VERSION`.
- Формат входных данных для инференса совпадает с тем, что отдает API.

### 📦 Результат:
- Обученная модель, доступная в MLflow UI.
- Скрипт обучения, который можно запустить вручную или через Cron.

---

## 👤 Агент 4: Application Developer (API + C++)

**Директория работы:** `api/` и `simulators/cpp_qt5/`  
**Ответственность:** Сервис предсказаний (FastAPI) и симуляторы (C++ Qt5).

### Часть А: FastAPI Server (`api/`)
1. **Main App (`src/main.py`)**:
   - Endpoint `POST /predict`: Принимает `entity_id` и request-time features.
   - Endpoint `GET /models`: Список доступных моделей из MLflow.
   - Endpoint `GET /health`: Проверка статуса.

2. **Логика предсказания (`services/predictor.py`)**:
   - Загрузка модели из MLflow (кэширование).
   - Получение исторических фич из Feast/Redis (`get_historical_features`).
   - Формирование полной последовательности (история + текущие значения).
   - Запуск `model.forward()` и возврат результата.

3. **Логирование**:
   - Отправка факта инференса в Kafka топик `inference_logs`.

### Часть Б: C++ Симуляторы (`simulators/cpp_qt5/`)
1. **Telemetry Daemon (`src/main_daemon.cpp`)**:
   - Генерация JSON событий с полями: `ts`, `id`, `temp`, `rpm`.
   - Отправка в Kafka через `librdkafka`.
   - Имитация аномалий (редкие скачки значений).

2. **Control Panel GUI (`src/control_panel.cpp`)**:
   - Интерфейс на Qt Widgets/QML.
   - Ввод параметров (ползунки).
   - HTTP запрос к API `/predict` через `QNetworkAccessManager`.
   - Отрисовка графика ответов (QChart).

3. **CMakeLists.txt**:
   - Поиск пакетов: `Qt5Core`, `Qt5Network`, `Qt5Charts`, `RdKafka`.
   - Сборка двух исполняемых файлов.

### 🔗 Точки интеграции:
- API читает модель, созданную Агентом 3.
- API читает фичи, записанные Агентом 2.
- C++ шлет данные в Kafka, настроенную Агентом 1.

### 📦 Результат:
- Рабочий Swagger UI по адресу `http://localhost:8000/docs`.
- Скомпилированные бинарники симуляторов.

---

## 🔄 Процесс сборки и интеграции

После выполнения задач всеми агентами, главный инженер (User/AI) выполнит:

1. **Проверка структуры**:
   ```bash
   tree -L 3
   ```
2. **Запуск инфраструктуры**:
   ```bash
   cd infra
   docker-compose up --build
   ```
3. **Верификация потоков**:
   - Убедиться, что данные идут: C++ -> Kafka -> Flink -> Redis.
4. **Тестирование ML**:
   - Запустить обучение (Агент 3).
   - Сделать запрос к API (Агент 4).

---

## 📝 Глоссарий терминов

- **Entity ID**: Уникальный идентификатор судна (например, `VSL-001`).
- **Request-time features**: Признаки, известные только в момент запроса (например, прогноз погоды).
- **Point-in-Time Join**: Корректное соединение истории признаков с целевой переменной без заглядывания в будущее.
- **Iceberg**: Формат табличного хранения данных поверх S3/MinIO.

---

**Внимание всем агентам:** При создании файлов используйте относительные пути от корня проекта. Не создавайте файлы вне своих папок. Для взаимодействия между модулями используйте только определенные интерфейсы (Kafka, Redis, REST, MLflow).
