# Universal Feature Store (UFS): Полная система управления признаками и ML-моделями

## 📌 Описание

**Universal Feature Store** — это полноценная платформа для управления признаками (Feature Store), обслуживания ML-моделей (Model Serving) и автоматического переобучения на примере телеметрии промышленного оборудования.

Система использует **временные ряды** и **RNN/LSTM/GRU** модели для предсказания отказов оборудования.

## 🏗️ Архитектура (13 сервисов)

### Уровень Инфраструктуры
| Сервис | Порт | Назначение |
|--------|------|------------|
| zookeeper | 2181 | Координатор для Kafka |
| kafka | 9092 | Брокер сообщений |
| redis | 6379 | Online Store (горячие данные) |
| minio | 9000/9001 | Offline Store (Apache Iceberg) |
| mlflow | 5000 | Реестр моделей |

### Уровень Обработки
| Сервис | Порт | Назначение |
|--------|------|------------|
| flink-jobmanager | 8081 | Мастер-нода Flink |
| flink-taskmanager | - | Воркер Flink |
| feast-online-server | 6566 | gRPC сервер Feast |

### Уровень Приложений
| Сервис | Порт | Назначение |
|--------|------|------------|
| api-server | 8000 | FastAPI шлюз |
| trainer | - | Сервис переобучения |
| drift-detector | - | Мониторинг дрейфа |

### Уровень Симуляции
| Сервис | Назначение |
|--------|------------|
| telemetry-daemon | C++ симулятор телеметрии |
| init-job | Инициализация при старте |

### Уровень Мониторинга
| Сервис | Порт | Назначение |
|--------|------|------------|
| prometheus | 9090 | Сбор метрик |
| grafana | 3000 | Визуализация |

## 🚀 Быстрый старт

```bash
cd /workspace/universal-feature-store

# Запуск всех 13 сервисов
docker-compose up --build

# Проверка здоровья API
curl http://localhost:8000/health

# Предсказание с использованием LSTM модели
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "lstm_failure_predictor",
    "entity_id": "ENTITY-001",
    "request_features": {"load_factor": 0.85},
    "sequence_data": [...]
  }'
```

## 📂 Структура проекта

```
universal-feature-store/
├── docker-compose.yml          # Конфигурация всех сервисов
├── src/
│   ├── api/
│   │   └── main.py            # FastAPI сервер с LSTM/RNN поддержкой
│   ├── processor/
│   │   └── flink_job.py       # Flink job для оконных агрегатов
│   ├── ml/
│   │   ├── lstm_models.py     # LSTM, GRU, CNN модели
│   │   └── telemetry_generator.py  # Генератор временных рядов
│   └── monitoring/
├── imitators/cpp_qt5/         # C++ симуляторы
│   ├── src/
│   ├── include/
│   ├── CMakeLists.txt
│   └── README.md
├── config/
├── scripts/
└── docs/
```

## 🔑 Ключевые возможности

### 1. Генерация временных рядов
- Синусоидальные паттерны с трендом и шумом
- Аномалии различных типов (temperature_spike, rpm_drop, pressure_loss)
- Реалистичные метрики оборудования

### 2. RNN/LSTM Модели
- **LSTMFailurePredictor**: Bidirectional LSTM с Attention механизмом
- **GRUFailurePredictor**: Более легкая GRU альтернатива
- **TemporalCNN**: 1D CNN для обработки временных рядов
- **EnsembleModel**: Комбинация моделей

### 3. Потоковая обработка (Flink)
- Tumbling окна (1 час) для средних значений
- Hopping окна (10 мин) для стандартного отклонения
- Session окна для обнаружения аномалий

### 4. API Endpoints
- `POST /predict` - Предсказание с последовательностями
- `GET /models` - Список доступных моделей
- `GET /features/{entity_id}` - Получение признаков
- `GET /anomaly/history/{entity_id}` - История аномалий

## 📊 ML Модели

### LSTM Failure Predictor
```python
model = LSTMFailurePredictor(
    input_size=8,        # Количество признаков
    hidden_size=128,     # Размер скрытого слоя
    num_layers=2,        # Количество LSTM слоев
    dropout=0.3,         # Dropout rate
    bidirectional=True   # Bidirectional LSTM
)
```

### Признаки для обучения
1. temperature_primary
2. rpm_main
3. speed_current
4. fuel_level
5. vibration_index
6. pressure_oil
7. flow_coolant
8. temperature_exhaust

## 🔧 Технологии

| Компонент | Технология |
|-----------|------------|
| Backend | Python 3.10+, FastAPI |
| C++ Simulator | C++11, Qt5 |
| Stream Processing | Apache Flink 1.17 |
| Message Broker | Apache Kafka 3.x |
| Online Store | Redis 7.x |
| Offline Store | Apache Iceberg on MinIO |
| ML Framework | PyTorch, MLflow |
| Monitoring | Prometheus + Grafana |

## 📚 Документация

- [Installation Guide](docs/02_Installation_Guide.md)
- [User Guide](docs/03_User_Guide_Beginner.md)
- [Integration Guide](docs/04_Integration_Guide.md)
- [MLOps Retraining](docs/06_MLOps_Retraining.md)

## Лицензия

MIT License
