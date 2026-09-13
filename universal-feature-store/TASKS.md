# Ship Feature Store: План распределения задач между агентами

## 📋 Общая архитектура системы

Система состоит из 4 уровней, которые должны быть связаны через четко определенные интерфейсы:

```
┌─────────────────────────────────────────────────────────────────┐
│  Уровень 1: Инфраструктура (Data & Storage)                     │
│  Агент: @InfrastructureAgent                                    │
├─────────────────────────────────────────────────────────────────┤
│  Уровень 2: Обработка данных (Stream Processing)                │
│  Агент: @StreamingAgent                                         │
├─────────────────────────────────────────────────────────────────┤
│  Уровень 3: ML модели (RNN/LSTM + Serving)                      │
│  Агент: @MLAgent                                                │
├─────────────────────────────────────────────────────────────────┤
│  Уровень 4: Приложения (C++ симуляторы + API)                   │
│  Агент: @ApplicationAgent                                       │
└─────────────────────────────────────────────────────────────────┘
```

## 🔗 Точки интеграции (Interfaces)

### Interface 1: Kafka Topics (Связь Infrastructure ↔ Streaming)
- **Топик `vessel_events`**: C++ daemon → Flink
  - Формат: `{"entity_id": "VSL-001", "timestamp": 1234567890, "features": {...}}`
- **Топик `inference_logs`**: API → Flink/Trainer
  - Формат: `{"request": {...}, "prediction": {...}, "timestamp": ...}`

### Interface 2: Redis Keys (Связь Streaming ↔ ML Serving)
- **Ключи Redis**: `feature_store:{entity_id}:{feature_name}`
  - Пример: `feature_store:VSL-001:engine_temp_avg_1h`
  - TTL: 2 часа

### Interface 3: Feast Feature Store (Связь Streaming ↔ API)
- **Feature View**: `vessel_telemetry_features`
  - Entity: `entity_id` (string)
  - Features: `engine_temp_avg_1h`, `rpm_std_10min`, и т.д.

### Interface 4: MLflow Models (Связь ML ↔ API)
- **Model URI**: `models:/lstm_engine_failure/Production`
  - Вход: последовательность длиной 60 шагов × 8 признаков
  - Выход: `{failure_probability: float, remaining_useful_life: int}`

### Interface 5: REST API (Связь API ↔ C++ GUI)
- **Endpoint**: `POST /predict`
  - Request: `{"model_name": "...", "entity_id": "...", "sequence": [...]}`
  - Response: `{"prediction": {...}, "latency_ms": ...}`

---

## 🎯 Промты для агентов

### Промт 1: @InfrastructureAgent

```markdown
# ЗАДАЧА: Настроить инфраструктуру контейнеров для Ship Feature Store

## Контекст
Тебе нужно создать Docker Compose конфигурацию для 13 сервисов, которые обеспечат работу платформы управления признаками и ML-моделями для телеметрии судна.

## Требования

### 1. Создай файл `docker-compose.yml` со следующими сервисами:

#### Уровень инфраструктуры:
- **zookeeper** (порт 2181): Apache Zookeeper для координации Kafka
- **kafka** (порт 9092, 29092): Kafka broker с топиками `vessel_events` и `inference_logs`
- **redis** (порт 6379): Redis для online store (feature store)
- **minio** (порты 9000, 9001): S3-совместимое хранилище для Iceberg (offline store)
- **mlflow** (порт 5000): MLflow сервер для реестра моделей

#### Уровень обработки:
- **flink-jobmanager** (порт 8081): Flink job manager
- **flink-taskmanager** (1 реплика): Flink task manager для потоковой обработки
- **feast-online-server** (порт 6566): Feast gRPC сервер для доступа к фичам

#### Уровень приложений:
- **api-server** (порт 8000): FastAPI сервер для предсказаний
- **trainer** (без порта): Сервис переобучения моделей
- **drift-detector** (без порта): Детектор дрейфа данных
- **cpp-telemetry-daemon** (без порта): C++ демон генерации телеметрии
- **init-job** (одноразовый): Инициализация топиков Kafka и Feast

#### Уровень мониторинга:
- **prometheus** (порт 9090): Сбор метрик
- **grafana** (порт 3000): Визуализация метрик

### 2. Для каждого сервиса укажи:
- Образ Docker (используй конкретные версии, например `apache/flink:1.17-scala_2.11-java8`)
- Переменные окружения (env)
- Ports mapping
- Volumes для персистентности данных
- Dependencies (depends_on)
- Health checks где применимо

### 3. Создай файлы конфигурации:
- `config/kafka/topics.json` - определение топиков Kafka
- `config/feast/feature_store.yaml` - конфигурация Feast
- `config/mlflow/mlflow_conf.yaml` - настройки MLflow
- `deploy/grafana/datasources.yml` - datasource Prometheus для Grafana

### 4. Создай скрипты:
- `scripts/init-kafka.sh` - создание топиков Kafka
- `scripts/init-feast.sh` - применение конфигурации Feast
- `scripts/save_images.sh` - сохранение образов для оффлайн деплоя
- `scripts/load_images.sh` - загрузка образов на целевой машине

## Ограничения
- Все сервисы должны быть в одной сети `ship-network`
- Используй volumes для сохранения данных при перезапуске
- Предусмотри环境变量 для легкой смены конфигурации
- Минимум 2GB RAM выделено для Flink

## Результат
Готовый к запуску `docker-compose up --build`, который поднимет все 13 сервисов.

## Точки интеграции (ВАЖНО!)
- Kafka broker должен быть доступен по адресу `kafka:9092` внутри сети и `localhost:9092` снаружи
- Redis должен хранить ключи в формате `feature_store:{entity_id}:{feature_name}`
- MLflow должен сохранять модели в MinIO (S3-compatible storage)
- Feast должен читать фичи из Redis и писать конфигурацию в MinIO
```

---

### Промт 2: @StreamingAgent

```markdown
# ЗАДАЧА: Реализовать потоковую обработку данных на Apache Flink

## Контекст
Тебе нужно создать PyFlink job, который будет обрабатывать поток телеметрии от судна, вычислять оконные агрегаты и обновлять feature store в реальном времени.

## Требования

### 1. Создай файл `src/processor/flink_job.py`:

#### Источник данных (Source):
- Чтение из Kafka топика `vessel_events`
- Формат сообщения: 
  ```json
  {
    "entity_id": "VSL-001",
    "timestamp": 1699900000,
    "features": {
      "engine_temperature": 85.5,
      "engine_rpm": 1200,
      "vessel_speed": 12.3,
      "fuel_level": 78.5,
      "vibration_level": 0.45,
      "oil_pressure": 3.2,
      "coolant_flow": 150.0,
      "exhaust_temperature": 420.0
    }
  }
  ```

#### Операции обработки:
- **Watermarking**: обработка опоздавших событий (до 5 секунд)
- **KeyBy**: группировка по `entity_id`
- **Оконные агрегаты**:
  - Tumbling окно 1 час: среднее значение (`_avg_1h`)
  - Hopping окно 10 минут (шаг 5 мин): стандартное отклонение (`_std_10min`)
  - Session окно (gap 30 мин): количество событий (`_count_session`)
- **Вычисление производных признаков**:
  - `temp_rpm_ratio`: отношение температуры к оборотам
  - `vibration_trend`: тренд вибрации за последние 5 измерений

#### Стоки данных (Sinks):
- **Redis Sink**: запись онлайн-фичей с TTL 2 часа
  - Ключ: `feature_store:{entity_id}:{feature_name}`
  - Пример: `SET feature_store:VSL-001:engine_temp_avg_1h 87.3 EX 7200`
- **Iceberg Sink**: запись исторических данных в MinIO
  - Путь: `s3://feature-store/vessel_telemetry/date=YYYY-MM-DD/`
  - Формат: Parquet с партиционированием по дате и entity_id

### 2. Создай файл `src/processor/backfill_job.py`:
- Чтение исторических данных из MinIO/Iceberg
- Пересчет агрегатов за прошлые периоды
- Запись в Redis для заполнения online store

### 3. Создай файл `src/processor/schema_registry.py`:
- Валидация схемы входящих сообщений
- Версионирование схем
- Логирование ошибок валидации

## Ограничения
- Задержка обработки < 100 мс (p95)
- Поддержка throughput до 10,000 событий/секунду
- Автоматический restart при падении задачи
- Checkpointing каждые 30 секунд в MinIO

## Результат
Готовый PyFlink job, который можно запустить через `flink run -py src/processor/flink_job.py`

## Точки интеграции (ВАЖНО!)
- **Вход**: Kafka topic `vessel_events` (JSON messages)
- **Выход 1**: Redis keys `feature_store:*` (строковые значения с TTL)
- **Выход 2**: Iceberg table `vessel_telemetry` в MinIO (Parquet файлы)
- **Метрики**: Expose Prometheus metrics на порту 9249

## Пример кода для начала:
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment

def create_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    # TODO: Добавить источник Kafka
    # TODO: Добавить оконные операции
    # TODO: Добавить стоки в Redis и Iceberg
    env.execute("Vessel Telemetry Processing")
```
```

---

### Промт 3: @MLAgent

```markdown
# ЗАДАЧА: Реализовать ML модели (RNN/LSTM) и сервис обучения

## Контекст
Тебе нужно создать модуль для работы с временными рядами телеметрии судна, включая LSTM/GRU модели для предсказания отказов двигателя и остаточного ресурса (RUL).

## Требования

### 1. Создай файл `src/ml/lstm_models.py`:

#### Модели:
- **LSTM_EngineFailure**: Binary classification (отказ/нет отказа)
  - Вход: последовательность 60 шагов × 8 признаков
  - Архитектура: 
    - Bidirectional LSTM (128 units)
    - Attention layer
    - Dense (64, activation='relu')
    - Dropout (0.3)
    - Output: sigmoid (вероятность отказа)
  
- **LSTM_RUL_Predictor**: Regression (остаточный ресурс в часах)
  - Вход: последовательность 60 шагов × 8 признаков
  - Архитектура:
    - GRU (256 units, return_sequences=True)
    - GRU (128 units)
    - Dense (64, activation='relu')
    - Output: linear (часы до отказа)

- **CNN_LSTM_Hybrid**: Hybrid model для обнаружения аномалий
  - 1D CNN слои для extraction локальных паттернов
  - LSTM для временных зависимостей
  - Autoencoder bottleneck для anomaly detection

#### Функции:
- `create_sequences(data, seq_length=60)`: создание последовательностей из временного ряда
- `prepare_telemetry_data(raw_data)`: нормализация, обработка пропусков, feature engineering
- `train_model(model_type, train_data, val_data, config)`: обучение с early stopping
- `load_model_from_mlflow(model_uri)`: загрузка модели из MLflow
- `save_model_to_mlflow(model, metrics, params)`: сохранение в MLflow registry

### 2. Создай файл `src/ml/trainer.py`:
- Чтение обучающих данных из Iceberg (MinIO)
- Split на train/validation/test (70/15/15)
- Обучение модели с логированием метрик в MLflow
- Валидация: ROC-AUC для classification, MAE для regression
- Регистрация лучшей модели в MLflow с тегом `Production`

### 3. Создай файл `src/ml/feature_engineering.py`:
- Статистические признаки: mean, std, min, max, skewness, kurtosis
- Временные признаки: hour_of_day, day_of_week, is_night
- Лаговые признаки: lag_1, lag_5, lag_10
- Скользящие статистики: rolling_mean_5, rolling_std_10

### 4. Создай файл `src/ml/model_manager.py`:
- LRU cache для загруженных моделей (max 10 моделей в памяти)
- Lazy loading из MLflow при первом запросе
- Hot reload при обновлении модели в registry
- Метрики: cache_hit_rate, load_latency_ms

## Ограничения
- Время инференса < 50 мс (p95)
- Поддержка batch inference до 100 последовательностей
- Модель должна помещаться в 500 MB RAM
- Обучение должно завершаться за < 30 минут на dataset 1M записей

## Результат
Готовые классы моделей и функции для обучения/инференса, интегрированные с MLflow.

## Точки интеграции (ВАЖНО!)
- **Вход для обучения**: Iceberg table `vessel_telemetry` с лейблами
- **Вход для инференса**: последовательность shape (60, 8) normalized
- **Выход**: dict `{"failure_probability": 0.85, "rul_hours": 120, "anomaly_score": 0.12}`
- **Хранение**: MLflow artifacts в MinIO (`s3://mlflow-models/...`)

## Пример кода для начала:
```python
import torch
import torch.nn as nn

class LSTM_EngineFailure(nn.Module):
    def __init__(self, input_size=8, hidden_size=128, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, 
                           batch_first=True, bidirectional=True)
        self.attention = nn.Linear(hidden_size * 2, 1)
        self.fc = nn.Sequential(
            nn.Linear(hidden_size * 2, 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        # TODO: Добавить attention mechanism
        # TODO: Добавить fully connected layers
        return prediction
```
```

---

### Промт 4: @ApplicationAgent

```markdown
# ЗАДАЧА: Реализовать API сервер и C++ симуляторы

## Контекст
Тебе нужно создать FastAPI сервер для обслуживания моделей и C++ приложения (демон и GUI) для генерации телеметрии и взаимодействия с системой.

## Часть 1: FastAPI Server

### 1. Создай файл `src/api/main.py`:

#### Endpoints:
- `GET /health`: проверка здоровья сервиса
  - Response: `{"status": "healthy", "models_loaded": 3}`

- `POST /predict`: предсказание для одной последовательности
  - Request:
    ```json
    {
      "model_name": "lstm_engine_failure",
      "entity_id": "VSL-001",
      "sequence": [[85.5, 1200, ...], [86.1, 1210, ...], ...],
      "request_features": {"wave_height": 2.5, "throttle_position": 0.7}
    }
    ```
  - Response:
    ```json
    {
      "prediction": {
        "failure_probability": 0.85,
        "rul_hours": 120,
        "anomaly_score": 0.12
      },
      "features_used": ["engine_temp_avg_1h", "rpm_std_10min"],
      "model_version": "v2.3",
      "latency_ms": 45
    }
    ```

- `POST /predict_batch`: пакетное предсказание
  - Request: список последовательностей
  - Response: список предсказаний

- `GET /models`: список доступных моделей
  - Response: `[{"name": "lstm_engine_failure", "version": "v2.3"}, ...]`

- `POST /feedback`: отправка feedback данных для дообучения
  - Request: `{"entity_id": "...", "actual_outcome": "...", "timestamp": ...}`

#### Логика:
- Загрузка модели из MLflow (через ModelManager)
- Запрос исторических фичей из Feast/Redis
- Конкатенация historical + request-time features
- Инференс модели
- Логирование в Kafka топик `inference_logs`

### 2. Создай файл `src/api/middleware.py`:
- Логирование всех запросов
- Metrics collection (Prometheus)
- Rate limiting (100 запросов/мин на IP)
- CORS configuration

## Часть 2: C++ Симуляторы

### 3. Создай структуру `imitators/cpp_qt5/`:

#### Файлы:
- `CMakeLists.txt`: сборка Daemon и GUI приложений
- `src/main_daemon.cpp`: точка входа консольного демона
- `src/telemetry_generator.cpp`: генерация временных рядов с аномалиями
- `src/kafka_producer.cpp`: отправка в Kafka
- `src/main_gui.cpp`: точка входа GUI приложения
- `src/control_panel.cpp`: логика UI (слайдеры, графики)
- `include/telemetry_generator.h`: заголовки

#### Функционал Daemon:
- Генерация событий каждую секунду
- Синтез реалистичных значений с трендами и шумом
- Редкие аномалии (5% событий)
- Отправка в Kafka `vessel_events`

#### Функционал GUI:
- Вкладка настроек (Kafka broker, API endpoint)
- Вкладка параметров судна (слайдеры для request-time features)
- Вкладка мониторинга (графики предсказаний в реальном времени)
- Вкладка логов (история запросов/ответов)

## Ограничения
- API response time < 100 мс (p95)
- C++ демон: потребление RAM < 50 MB
- C++ GUI: запуск < 2 секунд
- Поддержка Qt 5.12+

## Результат
- Работающий FastAPI сервер на порту 8000
- Скомпилируемые C++ приложения (daemon и gui)

## Точки интеграции (ВАЖНО!)
- **API → MLflow**: загрузка моделей по URI
- **API → Feast**: GET features по entity_id
- **API → Kafka**: write inference logs
- **C++ Daemon → Kafka**: write vessel_events
- **C++ GUI → API**: HTTP POST /predict

## Пример кода для API:
```python
from fastapi import FastAPI
from ml.model_manager import ModelManager

app = FastAPI()
model_manager = ModelManager()

@app.post("/predict")
async def predict(request: PredictionRequest):
    # TODO: Загрузить модель
    # TODO: Получить фичи из Feast
    # TODO: Выполнить инференс
    # TODO: Залогировать в Kafka
    return {"prediction": {...}}
```

## Пример кода для C++:
```cpp
// telemetry_generator.cpp
class TelemetryGenerator {
public:
    TelemetryData generateNext();
private:
    double addTrend(double base);
    double addNoise(double value);
    bool shouldInjectAnomaly();
};
```
```

---

## 📁 Структура проекта для всех агентов

```
/workspace/ship-feature-store/
├── docker-compose.yml              # @InfrastructureAgent
├── config/
│   ├── kafka/
│   │   └── topics.json             # @InfrastructureAgent
│   ├── feast/
│   │   └── feature_store.yaml      # @InfrastructureAgent
│   └── mlflow/
│       └── mlflow_conf.yaml        # @InfrastructureAgent
├── scripts/
│   ├── init-kafka.sh               # @InfrastructureAgent
│   ├── init-feast.sh               # @InfrastructureAgent
│   └── save_images.sh              # @InfrastructureAgent
├── src/
│   ├── processor/
│   │   ├── flink_job.py            # @StreamingAgent
│   │   ├── backfill_job.py         # @StreamingAgent
│   │   └── schema_registry.py      # @StreamingAgent
│   ├── ml/
│   │   ├── lstm_models.py          # @MLAgent
│   │   ├── trainer.py              # @MLAgent
│   │   ├── feature_engineering.py  # @MLAgent
│   │   └── model_manager.py        # @MLAgent
│   └── api/
│       ├── main.py                 # @ApplicationAgent
│       └── middleware.py           # @ApplicationAgent
├── imitators/cpp_qt5/
│   ├── CMakeLists.txt              # @ApplicationAgent
│   ├── src/
│   │   ├── main_daemon.cpp         # @ApplicationAgent
│   │   ├── telemetry_generator.cpp # @ApplicationAgent
│   │   ├── kafka_producer.cpp      # @ApplicationAgent
│   │   ├── main_gui.cpp            # @ApplicationAgent
│   │   └── control_panel.cpp       # @ApplicationAgent
│   └── include/
│       └── *.h                     # @ApplicationAgent
├── tests/
│   ├── test_integration.py         # Общий тест
│   └── test_api.py                 # Тесты API
└── docs/
    ├── ARCHITECTURE.md             # Описание архитектуры
    └── INTEGRATION_GUIDE.md        # Руководство по интеграции
```

---

## 🔄 Процесс сборки и интеграции

1. **Каждый агент выполняет свою задачу** независимо
2. **Интеграционный тест** проверяет все точки соединения:
   - Kafka → Flink → Redis
   - Flink → Iceberg
   - API → MLflow → Redis
   - C++ → Kafka
   - C++ → API
3. **Финальная сборка**: `docker-compose up --build`
4. **Smoke тесты**: curl запросы к API, проверка логов

---

## ✅ Критерии приемки для каждого агента

### @InfrastructureAgent
- [ ] `docker-compose up` поднимает все 13 сервисов
- [ ] Health checks проходят для всех сервисов
- [ ] Kafka топики создаются автоматически
- [ ] MinIO bucket создан и доступен

### @StreamingAgent
- [ ] Flink job читает из Kafka
- [ ] Агрегаты вычисляются корректно
- [ ] Данные пишутся в Redis и Iceberg
- [ ] Checkpointing работает

### @MLAgent
- [ ] LSTM модели обучаются на тестовых данных
- [ ] Модели сохраняются в MLflow
- [ ] Инференс работает < 50 мс
- [ ] Trainer запускается по расписанию

### @ApplicationAgent
- [ ] API принимает POST /predict
- [ ] C++ демон компилируется и отправляет в Kafka
- [ ] C++ GUI отображает графики
- [ ] Логи пишутся в Kafka

---

## 🚀 Следующие шаги

1. Скопируй этот план в файл `TASKS.md`
2. Передай каждый промт соответствующему агенту
3. После выполнения всеми агентами своих задач:
   - Запусти интеграционные тесты
   - Исправь ошибки интеграции
   - Задокументируй финальную систему
