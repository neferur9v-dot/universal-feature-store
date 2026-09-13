# Ship Feature Store: Задачи для Агентов-Разработчиков

## 📁 Структура проекта

```
ship-feature-store/
├── infra/              # Агент 1: Инфраструктура (Docker, Kafka, Redis, etc.)
├── streaming/          # Агент 2: Потоковая обработка (Flink, PyFlink)
├── ml/                 # Агент 3: ML модели (LSTM, GRU, обучение)
├── api/                # Агент 4: API сервер и C++ симуляторы
├── simulators/cpp_qt5/ # C++ код симуляторов
├── docs/               # Документация
├── tests/              # Unit тесты
├── integration/        # Интеграционные тесты
└── AGENTS_TASKS.md     # Этот файл
```

---

## 🤖 Агент 1: InfrastructureAgent

**Папка:** `infra/`  
**Ответственность:** Docker Compose, конфигурация сервисов, сеть, тома

### Задачи:
1. Создать `docker-compose.yml` с 13 сервисами:
   - zookeeper (2181)
   - kafka (9092)
   - redis (6379)
   - minio (9000, 9001)
   - mlflow (5000)
   - flink-jobmanager (8081)
   - flink-taskmanager
   - feast-online-server (6566)
   - api-server (8000)
   - trainer
   - drift-detector
   - cpp-telemetry-daemon
   - init-job
   - prometheus (9090)
   - grafana (3000)

2. Создать конфиги:
   - `infra/kafka/topics.json` - топики Kafka
   - `infra/feast/feature_store.yaml` - конфигурация Feast
   - `infra/minio/buckets.json` - бакеты MinIO
   - `infra/prometheus/prometheus.yml` - метрики
   - `infra/grafana/dashboards.json` - дашборды

3. Создать скрипты инициализации:
   - `infra/scripts/init_kafka.sh`
   - `infra/scripts/init_feast.sh`
   - `infra/scripts/init_mlflow.py`

### Критерии приемки:
- [ ] `docker-compose up --build` запускает все 13 сервисов
- [ ] Все сервисы видят друг друга по именам
- [ ] Kafka топики созданы автоматически
- [ ] Feast конфигурация применена
- [ ] MLflow готов к приему моделей

### Точки интеграции:
- Kafka bootstrap servers: `kafka:29092` (внутри сети), `localhost:9092` (снаружи)
- Redis host: `redis:6379`
- MinIO endpoint: `http://minio:9000`
- MLflow tracking URI: `http://mlflow:5000`
- Feast gRPC: `feast-online-server:6566`

---

## 🤖 Агент 2: StreamingAgent

**Папка:** `streaming/`  
**Ответственность:** PyFlink jobs, оконные агрегаты, запись в Redis/MinIO

### Задачи:
1. Создать `streaming/flink_job.py`:
   - Чтение из Kafka топика `vessel_events`
   - Парсинг JSON телеметрии
   - Оконные агрегаты:
     - Tumbling window (1 час): среднее, std, min, max
     - Hopping window (10 мин, шаг 5 мин): тренды
     - Session window (сессии активности)
   - Запись агрегатов в Redis (online store)
   - Запись сырых данных в MinIO/Iceberg (offline store)

2. Создать `streaming/backfill.py`:
   - Загрузка исторических данных из MinIO
   - Пересчет агрегатов для missing периодов

3. Создать `streaming/watermark_strategy.py`:
   - Обработка опоздавших событий
   - Настройка watermark для event time

### Критерии приемки:
- [ ] Flink job успешно читает из Kafka
- [ ] Агрегаты вычисляются корректно
- [ ] Данные записываются в Redis с ключом `feature_store:{entity_id}:{feature_name}`
- [ ] Iceberg таблицы созданы в MinIO
- [ ] Watermarks работают (опоздавшие события обрабатываются)

### Точки интеграции:
- Вход: Kafka `vessel_events`
- Выход Online: Redis keys `feature_store:*`
- Выход Offline: MinIO bucket `iceberg-data`, path `/warehouse/vessel_features`
- Формат данных: JSON с полями `entity_id`, `timestamp`, `features`

### Пример данных в Redis:
```
feature_store:VSL-001:engine_temp_avg_1h = 85.3
feature_store:VSL-001:engine_rpm_std_10m = 120.5
feature_store:VSL-001:last_update = 1699876543
```

---

## 🤖 Агент 3: MLAgent

**Папка:** `ml/`  
**Ответственность:** LSTM/GRU модели, обучение, регистрация в MLflow

### Задачи:
1. Создать `ml/models/lstm_model.py`:
   - Класс `LSTMEngineFailure` (nn.Module)
   - Слои: LSTM(128) → Dropout(0.3) → LSTM(64) → Attention → Dense(1)
   - Вход: последовательность длиной 60 шагов × 8 признаков
   - Выход: вероятность отказа (0-1)

2. Создать `ml/models/gru_model.py`:
   - Аналогично LSTM, но с GRU слоями

3. Создать `ml/models/cnn_lstm.py`:
   - CNN слои для извлечения признаков + LSTM для временной зависимости

4. Создать `ml/training/train.py`:
   - Загрузка данных из Iceberg (Offline Store)
   - Генерация последовательностей (`create_sequences()`)
   - Обучение с валидацией
   - Логирование метрик в MLflow

5. Создать `ml/training/data_preparation.py`:
   - Функции: `prepare_telemetry_data()`, `normalize_features()`, `create_sequences()`
   - Обработка пропусков, нормализация

6. Создать `ml/inference/predictor.py`:
   - Загрузка модели из MLflow
   - Предсказание на последовательности
   - Возврат вероятности + объяснение (SHAP значения)

### Критерии приемки:
- [ ] Модели обучаются на синтетических данных
- [ ] Метрики (AUC, Precision, Recall) логируются в MLflow
- [ ] Модели регистрируются в MLflow Model Registry
- [ ] Predictor загружает модель и делает инференс < 50мс
- [ ] Поддержка 3 типов моделей: LSTM, GRU, CNN-LSTM

### Точки интеграции:
- Вход для обучения: MinIO/Iceberg `warehouse/vessel_features`
- Вход для инференса: последовательность 60×8 признаков
- Выход: модель в MLflow с именем `lstm_engine_failure_v{version}`
- MLflow URI: `http://mlflow:5000`

### Пример входных данных для предсказания:
```python
sequence = np.array([
    [85.2, 1200, 12.5, 78.3, 0.45, 3.2, 150.0, 420.0],  # t-59
    [85.5, 1210, 12.6, 78.1, 0.46, 3.3, 151.0, 422.0],  # t-58
    ...
    [87.1, 1250, 12.8, 77.5, 0.52, 3.1, 148.0, 430.0],  # t-0
])  # shape: (60, 8)

# Признаки: [temp, rpm, speed, fuel, vibration, oil_pressure, coolant_flow, exhaust_temp]
```

---

## 🤖 Агент 4: ApplicationAgent

**Папка:** `api/` и `simulators/cpp_qt5/`  
**Ответственность:** FastAPI сервер, C++ симуляторы (Daemon + GUI)

### Задачи для API:

1. Создать `api/main.py`:
   - FastAPI приложение
   - Endpoints:
     - `GET /health` - проверка здоровья
     - `POST /predict` - предсказание для одной модели
     - `POST /predict/batch` - пакетное предсказание
     - `GET /models` - список доступных моделей
     - `POST /models/{name}/load` - загрузка модели
     - `GET /features/{entity_id}` - получение фич из Feast

2. Создать `api/model_manager.py`:
   - LRU cache для моделей
   - Загрузка из MLflow при старте или по требованию
   - Hot reload при обновлении модели

3. Создать `api/feature_store_client.py`:
   - Клиент для Feast gRPC сервера
   - Получение online фичей для entity_id
   - Merge с request-time features

4. Создать `api/logging.py`:
   - Логирование каждого инференса в Kafka `inference_logs`
   - Формат: `{"model": "...", "entity_id": "...", "features": {...}, "prediction": ..., "timestamp": ...}`

### Задачи для C++ симуляторов:

5. Создать `simulators/cpp_qt5/CMakeLists.txt`:
   - Сборка двух приложений: `ship_telemetry_daemon` и `ship_control_panel`
   - Зависимости: Qt5 Core, Qt5 Network, Qt5 Charts, librdkafka, spdlog

6. Создать `simulators/cpp_qt5/src/main_daemon.cpp`:
   - Чтение конфига
   - Подключение к Kafka
   - Генерация телеметрии (температура, RPM, скорость, топливо)
   - Отправка в Kafka топик `vessel_events`
   - Логирование

7. Создать `simulators/cpp_qt5/src/main_gui.cpp`:
   - Qt Widgets приложение
   - Вкладки: Настройки, Параметры судна, Мониторинг, Логи
   - Слайдеры для request-time features
   - Графики предсказаний (Qt Charts)
   - HTTP клиент для запросов к API

8. Создать `simulators/cpp_qt5/include/telemetry_generator.h`:
   - Класс для генерации реалистичной телеметрии
   - Добавление шума и аномалий

### Критерии приемки:
- [ ] API запускается на порту 8000
- [ ] `/predict` endpoint возвращает предсказание < 100мс
- [ ] Модели загружаются из MLflow
- [ ] Фичи берутся из Feast/Redis
- [ ] Инференсы логируются в Kafka
- [ ] C++ демон компилируется и отправляет данные в Kafka
- [ ] C++ GUI компилируется, отображает графики и отправляет запросы

### Точки интеграции:
- API слушает: `0.0.0.0:8000`
- Feast gRPC: `feast-online-server:6566`
- MLflow: `http://mlflow:5000`
- Kafka для логов: `inference_logs`
- C++ демон → Kafka: `vessel_events`
- C++ GUI → API: `http://api-server:8000/predict`

### Пример запроса к API:
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "lstm_engine_failure",
    "entity_id": "VSL-001",
    "sequence_length": 60,
    "request_features": {
      "wave_height": 2.5,
      "throttle_position": 0.75
    }
  }'
```

---

## 🔗 Интеграция всех агентов

### После выполнения задач всеми агентами:

1. **Проверка инфраструктуры** (Агент 1):
   ```bash
   cd ship-feature-store/infra
   docker-compose up --build -d
   docker-compose ps  # все 13 сервисов должны быть Up
   ```

2. **Запуск потоковой обработки** (Агент 2):
   ```bash
   # Flink job автоматически запускается через docker-compose
   # Проверка в UI Flink: http://localhost:8081
   ```

3. **Обучение моделей** (Агент 3):
   ```bash
   python ml/training/train.py --model lstm --epochs 10
   # Модель появится в MLflow: http://localhost:5000
   ```

4. **Запуск API и симуляторов** (Агент 4):
   ```bash
   # API уже запущен через docker-compose
   # Сборка C++:
   cd simulators/cpp_qt5
   mkdir build && cd build
   cmake .. && make -j4
   ./ship_telemetry_daemon  # в одном терминале
   ./ship_control_panel     # в другом терминале
   ```

### Интеграционные тесты:

Создать файл `integration/test_full_pipeline.py`:

```python
def test_end_to_end():
    # 1. Отправить телеметрию через C++ daemon (или эмуляцию)
    # 2. Проверить, что Flink обработал и записал в Redis
    # 3. Сделать запрос к API /predict
    # 4. Проверить ответ
    # 5. Проверить, что лог инференса появился в Kafka
    # 6. Проверить, что модель загружена из MLflow
    pass
```

### Общая схема потока данных:

```
C++ Daemon → Kafka(vessel_events) → Flink → Redis + MinIO/Iceberg
                                                       ↓
C++ GUI → API(/predict) → Feast(Redis) + MLflow(Model) → Prediction
                     ↓
              Kafka(inference_logs) → Trainer → MLflow(new model)
```

---

## 📝 Примечания для всех агентов

1. **Версии Python**: Использовать Python 3.10+ во всех сервисах
2. **Формат данных**: JSON с UTF-8 кодировкой
3. **Логирование**: Использовать структурированное логирование (JSON format)
4. **Обработка ошибок**: Graceful degradation, retry logic
5. **Конфигурация**: Все параметры через environment variables или config файлы
6. **Тесты**: Покрыть unit тестами критическую логику (минимум 70%)

---

## ✅ Чеклист готовности системы

- [ ] Все 13 сервисов запускаются через `docker-compose up`
- [ ] C++ симуляторы компилируются без ошибок
- [ ] Телеметрия поступает в Kafka
- [ ] Flink вычисляет агрегаты и пишет в Redis/MinIO
- [ ] Модели обучаются и регистрируются в MLflow
- [ ] API принимает запросы и возвращает предсказания
- [ ] Инференсы логируются в Kafka
- [ ] Grafana дашборды отображают метрики
- [ ] Интеграционные тесты проходят

---

**Интегрирующий агент** (я) соберет все компоненты в единую систему после выполнения задач всеми агентами.
