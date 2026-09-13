# Задание для Агента 2: Big Data Streaming Engineer

## 🎯 Роль
Вы отвечаете за потоковую обработку данных в реальном времени с помощью Apache Flink (PyFlink). Ваша задача — читать события из Kafka, вычислять оконные агрегаты и записывать результаты в Online Store (Redis) и Offline Store (Iceberg/MinIO).

## 📂 Расположение файлов
Все ваши файлы должны находиться в папке: `/workspace/ship-feature-store/streaming/`

## 📋 Список задач

### 1. PyFlink Job основной
Создайте файл `streaming/flink_job.py` — главный входной точка Flink задачи.

#### Требования к логике:
1. **Чтение из Kafka**:
   - Топик: `vessel_events`
   - Формат: JSON
   - Bootstrap servers: `kafka:29092`
   - Group ID: `flink-consumer-group`
   - Auto offset reset: `earliest`

2. **Парсинг и схема данных**:
   Определите схему входящих событий:
   ```python
   {
     "entity_id": "VSL-001",
     "timestamp": "2024-01-15T10:30:00Z",
     "engine_temperature": 85.5,
     "engine_rpm": 1200,
     "vessel_speed": 12.3,
     "fuel_level": 75.2,
     "vibration_level": 0.45,
     "oil_pressure": 3.2,
     "coolant_flow": 150.0,
     "exhaust_temperature": 420.5
   }
   ```

3. **Назначение водяных знаков (Watermarks)**:
   - Извлеките timestamp из события
   - Установите watermark с допустимой задержкой 5 секунд для обработки опоздавших событий

4. **Оконные агрегаты** (реализуйте 3 типа окон):

   **a) Tumbling Window (1 час)**:
   - Вычислите среднее значение (`avg`) для каждого признака
   - Ключ группировки: `entity_id`
   - Результат: `engine_temp_avg_1h`, `rpm_avg_1h`, и т.д.

   **b) Hopping Window (10 минут, шаг 5 минут)**:
   - Вычислите стандартное отклонение (`std`) для признаков
   - Результат: `engine_temp_std_10min`, `vibration_std_10min`

   **c) Session Window (сессионное окно, гейп 30 минут)**:
   - Вычислите минимальное и максимальное значения
   - Результат: `speed_min_session`, `speed_max_session`

5. **Запись в Redis (Online Store)**:
   - Формат ключа: `feature_store:{entity_id}:{feature_name}`
   - Пример: `feature_store:VSL-001:engine_temp_avg_1h`
   - TTL: 2 часа (для актуальности данных)
   - Используйте `Jedis` или `redis-py` connector для Flink

6. **Запись в Iceberg (Offline Store)**:
   - Путь: `s3://iceberg-data/warehouse/ship_features/telemetry`
   - Формат: Apache Iceberg с партиционированием по дате (`dt=date(timestamp)`)
   - Частота чекпоинтов: каждые 5 минут
   - Схема должна включать все исходные признаки + вычисленные агрегаты

### 2. Схема данных и сериализация
Создайте файл `streaming/schemas.py`:
- Определите классы данных для входных событий и агрегатов
- Реализуйте функции сериализации/десериализации JSON
- Добавьте валидацию схем (используйте `pydantic` или аналог)

### 3. Конфигурация Flink
Создайте файл `streaming/flink_config.yaml`:
```yaml
job:
  name: ship-telemetry-aggregation
  parallelism: 4
  checkpoint:
    interval: 300000  # 5 минут
    mode: EXACTLY_ONCE
    storage: s3://iceberg-data/warehouse/checkpoints

kafka:
  bootstrap_servers: kafka:29092
  topics:
    input: vessel_events
    output_logs: inference_logs

redis:
  host: redis
  port: 6379
  ttl_seconds: 7200

iceberg:
  warehouse: s3://iceberg-data/warehouse
  database: ship_features
  table: telemetry_history
```

### 4. UDF функции
Создайте файл `streaming/udfs.py` с пользовательскими функциями:
- `calculate_moving_average(values, window_size)`
- `calculate_standard_deviation(values)`
- `detect_anomaly(value, mean, std, threshold=3.0)` — возвращает boolean
- `normalize_feature(value, min_val, max_val)`

### 5. Скрипт запуска
Создайте файл `streaming/submit_job.sh`:
```bash
#!/bin/bash
# Отправка Flink job на кластер
FLINK_JOB_JAR=/opt/flink/usrlib/flink-job.py
FLINK_JOBMANAGER=flink-jobmanager:8081

curl -X POST http://$FLINK_JOBMANAGER/jars/upload \
  -F jarfile=@$FLINK_JOB_JAR

# Или через flink run, если используется Python API
# python -m pyflink.client.cli submit ...
```

### 6. Мониторинг и метрики
Добавьте в Flink job:
- Логирование количества обработанных событий в секунду
- Метрики задержки обработки (latency)
- Счетчики ошибок парсинга
- Интеграция с Prometheus через JMX exporter

## 🔗 Точки интеграции
- **Входные данные**: Kafka топик `vessel_events` (от C++ демона)
- **Выход Online**: Redis ключи `feature_store:*` (читает API сервер)
- **Выход Offline**: Iceberg таблица `s3://iceberg-data/warehouse/ship_features/telemetry` (читает Trainer)
- **Конфигурация**: Feast registry (должен совпадать с конфигом из Infra агента)

## ✅ Критерии приемки
1. Flink job успешно запускается через JobManager UI (http://localhost:8081)
2. Данные из Kafka читаются и парсятся без ошибок
3. Оконные агрегаты вычисляются корректно (проверьте через Redis CLI)
4. Данные записываются в Redis с правильным форматом ключей
5. Данные записываются в Iceberg (проверьте через Spark или Presto)
6. Watermarks обрабатывают опоздавшие события (тест с задержкой)
7. Чекпоинты создаются каждые 5 минут в MinIO
8. Метрики Flink доступны в Prometheus

## 🛠️ Технические требования
- Используйте PyFlink 1.17+ с поддержкой Python 3.9
- Для работы с Iceberg используйте `pyiceberg` библиотеку
- Обработайте исключения: ошибки подключения к Kafka, Redis, неверный формат JSON
- Код должен быть модульным и тестируемым (отдельные функции для окон, UDF, записи)

## 📝 Примечания
- Убедитесь, что зависимости (`requirements.txt`) включают: `apache-flink`, `redis`, `pyiceberg`, `kafka-python`
- Для локальной тестировки можно использовать MiniCluster Flink
- Предусмотрите возможность изменения размера окон через конфиг
