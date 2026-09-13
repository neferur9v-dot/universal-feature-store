# Задание для Агента 4: Application Developer (API + C++ Симуляторы)

## 🎯 Роль
Вы отвечаете за разработку приложений верхнего уровня: FastAPI сервер для обслуживания моделей и C++ симуляторы (консольный демон и GUI панель управления). Ваша задача — обеспечить интерфейс между пользователем и ML-системой.

## 📂 Расположение файлов
- **API сервер**: `/workspace/ship-feature-store/api/`
- **C++ симуляторы**: `/workspace/ship-feature-store/simulators/cpp_qt5/`

---

## ЧАСТЬ 1: FastAPI Server

### 1. Основное приложение
Создайте файл `api/main.py`:

#### Структура приложения:
```python
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Загрузка моделей при старте
    model_manager.load_models_from_mlflow()
    yield
    # Очистка при остановке

app = FastAPI(title="Ship Feature Store API", lifespan=lifespan)
```

#### Endpoints:
1. **GET /health** — проверка здоровья сервиса
   ```json
   {"status": "healthy", "models_loaded": 3}
   ```

2. **POST /predict** — получение предсказания
   ```python
   @app.post("/predict")
   async def predict(request: PredictionRequest):
       """
       PredictionRequest:
       {
         "model_name": "lstm_engine_failure",
         "entity_id": "VSL-001",
         "request_features": {
           "wave_height": 2.5,
           "throttle_position": 0.75
         },
         "sequence": [...]  # Опционально: готовая последовательность 60x8
       }
       
       Response:
       {
         "prediction": 0.85,
         "probability_of_failure": 0.85,
         "risk_level": "HIGH",
         "model_version": "v3",
         "features_used": {...},
         "inference_time_ms": 45
       }
       """
   ```

3. **GET /models** — список доступных моделей
   ```json
   [
     {"name": "lstm_engine_failure", "version": "v3", "metrics": {"auc_roc": 0.87}},
     {"name": "gru_fuel_efficiency", "version": "v1", "metrics": {"mae": 2.3}}
   ]
   ```

4. **POST /predict/batch** — батчевое предсказание
   ```python
   @app.post("/predict/batch")
   async def predict_batch(requests: List[PredictionRequest])
   ```

5. **GET /features/{entity_id}** — получение текущих фич из Feast
   ```json
   {
     "entity_id": "VSL-001",
     "features": {
       "engine_temp_avg_1h": 85.5,
       "rpm_std_10min": 12.3,
       ...
     }
   }
   ```

### 2. Менеджер моделей
Создайте файл `api/model_manager.py`:

#### Класс ModelManager:
```python
class ModelManager:
    def __init__(self, mlflow_uri: str):
        self.mlflow_uri = mlflow_uri
        self.loaded_models: Dict[str, LoadedModel] = {}
        self.lru_cache_size = 10
    
    def load_model(self, model_name: str, version: str = "latest") -> LoadedModel:
        """
        Загружает модель из MLflow:
        1. Проверяет кэш
        2. Если нет в кэше → загружает из MLflow
        3. Загружает веса (.pth), scaler, config
        4. Возвращает объект модели
        """
    
    def predict(self, model_name: str, input_data: np.ndarray) -> np.ndarray:
        """
        Выполняет инференс:
        1. Нормализует данные (использует scaler из MLflow)
        2. Создает последовательность (если нужно)
        3. Передает в модель
        4. Возвращает prediction
        """
    
    def _load_from_mlflow(self, model_name: str, version: str):
        """
        Скачивает артефакты из MLflow:
        - model.pth
        - scaler.pkl
        - config.yaml
        """
```

#### LRU кэш:
- Храните последние 10 загруженных моделей
- При превышении лимита выгружайте наименее используемую

### 3. Feast клиент
Создайте файл `api/feast_client.py`:

#### Получение онлайн фич:
```python
from feast import FeatureStore

class FeastFeatureClient:
    def __init__(self, feast_serving_url: str):
        self.store = FeatureStore(feast_serving_url)
    
    def get_online_features(self, entity_id: str, features: List[str]) -> dict:
        """
        Получает фичи из Redis через Feast gRPC сервер
        
        Returns:
          dict: {feature_name: value}
        """
        response = self.store.get_online_features(
            features=features,
            entity_rows=[{"entity_id": entity_id}]
        )
        return response.to_dict()
```

### 4. Логирование инференса
Создайте файл `api/inference_logger.py`:

#### Отправка логов в Kafka:
```python
from kafka import KafkaProducer
import json

class InferenceLogger:
    def __init__(self, kafka_broker: str):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def log(self, prediction_request: dict, prediction_result: dict):
        """
        Логирует каждый запрос для последующего переобучения:
        {
          "timestamp": "2024-01-15T10:30:00Z",
          "entity_id": "VSL-001",
          "model_name": "lstm_engine_failure",
          "input_features": {...},
          "prediction": 0.85,
          "model_version": "v3"
        }
        """
        log_entry = {**prediction_request, **prediction_result}
        self.producer.send('inference_logs', value=log_entry)
```

### 5. Конфигурация и Dockerfile
Создайте файлы:

#### `api/config.py`:
```python
from pydantic import BaseSettings

class Settings(BaseSettings):
    MLFLOW_TRACKING_URI: str = "http://mlflow:5000"
    REDIS_HOST: str = "redis"
    KAFKA_BROKER: str = "kafka:29092"
    FEAST_SERVING_URL: str = "feast-online-server:6566"
    MODEL_CACHE_SIZE: int = 10
    
    class Config:
        env_file = ".env"

settings = Settings()
```

#### `api/Dockerfile`:
```dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### `api/requirements.txt`:
```
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pydantic>=2.0.0
mlflow>=2.8.0
torch>=2.0.0
feast>=0.34.0
kafka-python>=2.0.0
redis>=5.0.0
numpy>=1.24.0
pandas>=2.0.0
```

---

## ЧАСТЬ 2: C++ Симуляторы (Qt5)

### 1. Структура проекта
Создайте структуру в `simulators/cpp_qt5/`:
```
simulators/cpp_qt5/
├── CMakeLists.txt
├── README.md
├── src/
│   ├── main_daemon.cpp      # Точка входа демона
│   ├── main_gui.cpp         # Точка входа GUI
│   ├── telemetry_generator.cpp
│   ├── kafka_producer.cpp
│   └── control_panel.cpp    # GUI логика
├── include/
│   ├── telemetry_generator.h
│   ├── kafka_producer.h
│   └── control_panel.h
└── resources/
    └── config.json
```

### 2. Ship Telemetry Daemon (Консольный)

#### Файл: `src/main_daemon.cpp`
```cpp
#include <QCoreApplication>
#include <QTimer>
#include "telemetry_generator.h"
#include "kafka_producer.h"

int main(int argc, char *argv[]) {
    QCoreApplication app(argc, argv);
    
    // Чтение конфига
    QString kafkaBroker = qgetenv("KAFKA_BROKER");
    if (kafkaBroker.isEmpty()) {
        kafkaBroker = "localhost:9092";
    }
    
    // Инициализация
    TelemetryGenerator generator;
    KafkaProducer producer(kafkaBroker.toStdString());
    
    // Таймер для отправки событий (1 секунда)
    QTimer timer;
    QObject::connect(&timer, &QTimer::timeout, [&]() {
        auto event = generator.generateEvent();
        producer.send("vessel_events", event);
        qDebug() << "Sent event:" << event["entity_id"];
    });
    timer.start(1000);  // 1 секунда
    
    return app.exec();
}
```

#### Файл: `src/telemetry_generator.cpp`
```cpp
#include "telemetry_generator.h"
#include <QJsonDocument>
#include <QJsonObject>
#include <QDateTime>
#include <QRandomGenerator>

QJsonObject TelemetryGenerator::generateEvent() {
    QJsonObject event;
    
    // Entity ID (VSL-001, VSL-002, ...)
    static int vesselCounter = 1;
    event["entity_id"] = QString("VSL-%1").arg(vesselCounter, 3, 10, QChar('0'));
    
    // Timestamp
    event["timestamp"] = QDateTime::currentDateTimeUtc().toString(Qt::ISODate);
    
    // Генерация реалистичных значений с синусоидальным паттерном
    double time = QDateTime::currentMSecsSinceEpoch() / 1000.0;
    
    // Температура двигателя (синус + тренд + шум)
    double baseTemp = 80.0 + 10.0 * sin(time * 0.01);  // Синусоида
    double trend = 0.001 * time;  // Медленный рост
    double noise = QRandomGenerator::global()->bounded(-2.0, 2.0);
    double temp = baseTemp + trend + noise;
    
    // Аномалии (редкие скачки)
    if (QRandomGenerator::global()->bounded(100) < 2) {  // 2% шанс
        temp += 30.0;  // Резкий скачок температуры
    }
    
    event["engine_temperature"] = temp;
    event["engine_rpm"] = 1200.0 + 200.0 * sin(time * 0.02) + QRandomGenerator::global()->bounded(-50, 50);
    event["vessel_speed"] = 12.0 + 3.0 * sin(time * 0.015);
    event["fuel_level"] = qMax(0.0, 75.0 - 0.01 * time);  // Постепенное снижение
    event["vibration_level"] = 0.4 + 0.1 * sin(time * 0.03);
    event["oil_pressure"] = 3.2 + 0.2 * sin(time * 0.01);
    event["coolant_flow"] = 150.0 + 10.0 * sin(time * 0.02);
    event["exhaust_temperature"] = 420.0 + 30.0 * sin(time * 0.015);
    
    return event;
}
```

#### Файл: `src/kafka_producer.cpp`
```cpp
#include "kafka_producer.h"
#include <librdkafka/rdkafkacpp.h>
#include <QJsonDocument>

KafkaProducer::KafkaProducer(const std::string& broker) {
    std::string errstr;
    
    conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    tconf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    
    conf_->set("metadata.broker.list", broker, errstr);
    conf_->set("produce.offset.report", "true", errstr);
    
    producer_ = RdKafka::Producer::create(conf_, errstr);
}

void KafkaProducer::send(const std::string& topic, const QJsonObject& data) {
    std::string message = QJsonDocument(data).toJson().toStdString();
    
    RdKafka::ErrorCode resp = producer_->produce(
        topic, RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY, 
        (void*)message.c_str(), message.size(),
        nullptr, nullptr
    );
    
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Kafka error: " << RdKafka::err2str(resp) << std::endl;
    }
    
    producer_->poll(0);
}
```

### 3. Ship Control Panel (GUI приложение)

#### Файл: `src/control_panel.cpp`
```cpp
#include "control_panel.h"
#include <QtWidgets/QApplication>
#include <QtWidgets/QMainWindow>
#include <QtWidgets/QTabWidget>
#include <QtWidgets/QSlider>
#include <QtWidgets/QLabel>
#include <QtCharts/QChartView>
#include <QtCharts/QLineSeries>
#include <QtNetwork/QNetworkAccessManager>
#include <QtNetwork/QNetworkReply>

ControlPanel::ControlPanel(QWidget *parent) : QMainWindow(parent) {
    setWindowTitle("Ship Control Panel");
    resize(1000, 700);
    
    // Создание вкладок
    QTabWidget* tabs = new QTabWidget(this);
    setCentralWidget(tabs);
    
    // Вкладка 1: Настройки
    QWidget* settingsTab = createSettingsTab();
    tabs->addTab(settingsTab, "Настройки");
    
    // Вкладка 2: Параметры судна
    QWidget* paramsTab = createParamsTab();
    tabs->addTab(paramsTab, "Параметры судна");
    
    // Вкладка 3: Мониторинг
    QWidget* monitoringTab = createMonitoringTab();
    tabs->addTab(monitoringTab, "Мониторинг");
    
    // Вкладка 4: Логи
    QWidget* logsTab = createLogsTab();
    tabs->addTab(logsTab, "Логи");
    
    // Network manager для API запросов
    networkManager_ = new QNetworkAccessManager(this);
}

QWidget* ControlPanel::createParamsTab() {
    QWidget* widget = new QWidget();
    QVBoxLayout* layout = new QVBoxLayout(widget);
    
    // Слайдер высоты волны
    QLabel* waveLabel = new QLabel("Высота волны (м): 2.5");
    QSlider* waveSlider = new QSlider(Qt::Horizontal);
    waveSlider->setRange(0, 100);  // 0.0 - 10.0 м
    waveSlider->setValue(25);      // 2.5 м
    
    connect(waveSlider, &QSlider::valueChanged, [=](int value) {
        double waveHeight = value / 10.0;
        waveLabel->setText(QString("Высота волны (м): %1").arg(waveHeight));
        currentWaveHeight_ = waveHeight;
    });
    
    // Слайдер положения дросселя
    QLabel* throttleLabel = new QLabel("Положение дросселя: 75%");
    QSlider* throttleSlider = new QSlider(Qt::Horizontal);
    throttleSlider->setRange(0, 100);
    throttleSlider->setValue(75);
    
    connect(throttleSlider, &QSlider::valueChanged, [=](int value) {
        throttleLabel->setText(QString("Положение дросселя: %1%").arg(value));
        currentThrottle_ = value / 100.0;
    });
    
    // Кнопка запроса предсказания
    QPushButton* predictBtn = new QPushButton("Запросить предсказание");
    connect(predictBtn, &QPushButton::clicked, this, &ControlPanel::requestPrediction);
    
    layout->addWidget(waveLabel);
    layout->addWidget(waveSlider);
    layout->addWidget(throttleLabel);
    layout->addWidget(throttleSlider);
    layout->addWidget(predictBtn);
    layout->addStretch();
    
    return widget;
}

void ControlPanel::requestPrediction() {
    // Формирование запроса к API
    QJsonObject request;
    request["model_name"] = selectedModel_;
    request["entity_id"] = "VSL-001";
    
    QJsonObject features;
    features["wave_height"] = currentWaveHeight_;
    features["throttle_position"] = currentThrottle_;
    request["request_features"] = features;
    
    // Отправка POST запроса
    QUrl url(apiUrl_ + "/predict");
    QNetworkRequest networkRequest(url);
    networkRequest.setHeader(QNetworkRequest::ContentTypeHeader, "application/json");
    
    QJsonDocument doc(request);
    QByteArray jsonData = doc.toJson();
    
    QNetworkReply* reply = networkManager_->post(networkRequest, jsonData);
    
    connect(reply, &QNetworkReply::finished, [=]() {
        if (reply->error() == QNetworkReply::NoError) {
            QByteArray response = reply->readAll();
            QJsonDocument responseDoc = QJsonDocument::fromJson(response);
            
            double probability = responseDoc.object()["probability_of_failure"].toDouble();
            
            // Обновление графика
            updateChart(probability);
            
            // Добавление в логи
            addLogEntry(QString("Prediction: %1 (Risk: %2)")
                .arg(probability)
                .arg(getRiskLevel(probability)));
        }
        reply->deleteLater();
    });
}

QWidget* ControlPanel::createMonitoringTab() {
    QWidget* widget = new QWidget();
    QVBoxLayout* layout = new QVBoxLayout(widget);
    
    // График с QChart
    chart_ = new QChart();
    chart_->setTitle("Вероятность отказа двигателя");
    chart_->createDefaultAxes();
    
    series_ = new QLineSeries();
    chart_->addSeries(series_);
    
    QChartView* chartView = new QChartView(chart_);
    chartView->setRenderHint(QPainter::Antialiasing);
    
    layout->addWidget(chartView);
    
    return widget;
}

void ControlPanel::updateChart(double probability) {
    static int pointCount = 0;
    series_->append(pointCount++, probability);
    
    // Автопрокрутка графика
    if (pointCount > 100) {
        series_->remove(0);
        pointCount--;
    }
}
```

### 4. CMakeLists.txt
```cmake
cmake_minimum_required(VERSION 3.10)
project(ShipSimulator VERSION 1.0)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

find_package(Qt5 COMPONENTS Core Network Charts Widgets REQUIRED)
find_package(RdKafka REQUIRED)

# Демон
add_executable(ship_telemetry_daemon
    src/main_daemon.cpp
    src/telemetry_generator.cpp
    src/kafka_producer.cpp
)
target_link_libraries(ship_telemetry_daemon
    Qt5::Core
    Qt5::Network
    RdKafka::rdkafka++
)

# GUI приложение
add_executable(ship_control_panel
    src/main_gui.cpp
    src/control_panel.cpp
    src/kafka_producer.cpp
)
target_link_libraries(ship_control_panel
    Qt5::Core
    Qt5::Network
    Qt5::Charts
    Qt5::Widgets
    RdKafka::rdkafka++
)

# Установка
install(TARGETS ship_telemetry_daemon ship_control_panel DESTINATION bin)
```

### 5. Dockerfile для C++ демона
```dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    qtbase5-dev \
    qtcharts5-dev \
    librdkafka-dev \
    libspdlog-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN mkdir build && cd build && cmake .. && make -j4

ENV KAFKA_BROKER=kafka:29092

CMD ["./build/ship_telemetry_daemon"]
```

## 🔗 Точки интеграции
- **API → MLflow**: Загрузка моделей по `MLFLOW_TRACKING_URI`
- **API → Feast**: Получение фич через gRPC на `FEAST_SERVING_URL`
- **API → Redis**: Прямые запросы (опционально)
- **API → Kafka**: Логирование инференсов в `inference_logs`
- **C++ Daemon → Kafka**: Отправка телеметрии в `vessel_events`
- **C++ GUI → API**: HTTP запросы на `/predict`

## ✅ Критерии приемки
1. API сервер запускается и отвечает на `/health`
2. Endpoint `/predict` возвращает корректные предсказания от LSTM модели
3. Модели загружаются из MLflow при старте API
4. Логи инференсов отправляются в Kafka
5. C++ демон генерирует события и отправляет в Kafka (проверьте через `kafka-console-consumer`)
6. C++ GUI отображает графики и позволяет отправлять запросы
7. Swagger UI доступен на `http://localhost:8000/docs`

## 🛠️ Технические требования
- FastAPI с асинхронными endpoint'ами
- C++11 стандарт, Qt5.12+
- Обработка ошибок подключения ко всем сервисам
- Логирование в stdout для Docker

## 📝 Примечания
- Для C++ используйте `qmake` или `CMake` (предпочтительно)
- Предусмотрите возможность горячей перезагрузки моделей в API
- Добавьте rate limiting для `/predict` endpoint
