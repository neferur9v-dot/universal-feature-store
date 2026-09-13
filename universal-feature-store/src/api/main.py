"""
FastAPI Server for Ship Feature Store
Provides prediction endpoints with LSTM/RNN model serving
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import numpy as np
import json
import asyncio
from datetime import datetime
import redis
import requests


app = FastAPI(
    title="Ship Feature Store API",
    description="API для предсказания отказов двигателя судна с использованием LSTM/RNN",
    version="1.0.0"
)


# ==================== Pydantic Models ====================

class PredictionRequest(BaseModel):
    """Запрос на предсказание"""
    model_name: str = Field(..., description="Название модели")
    entity_id: str = Field(..., description="ID судна")
    request_features: Dict[str, float] = Field(
        default_factory=dict, 
        description="Признаки времени запроса"
    )
    sequence_data: Optional[List[Dict[str, float]]] = Field(
        None, 
        description="Последовательность данных для RNN/LSTM"
    )


class PredictionResponse(BaseModel):
    """Ответ с предсказанием"""
    model_name: str
    entity_id: str
    prediction: float
    probability: float
    risk_level: str
    timestamp: str
    features_used: List[str]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    """Статус здоровья сервиса"""
    status: str
    services: Dict[str, bool]
    timestamp: str


class ModelInfo(BaseModel):
    """Информация о модели"""
    name: str
    type: str
    input_size: int
    sequence_length: int
    feature_names: List[str]
    version: str
    metrics: Dict[str, float]


# ==================== Global State ====================

# Кэш моделей
model_cache: Dict[str, Any] = {}

# Конфигурация подключений
REDIS_HOST = "redis"
REDIS_PORT = 6379
MLFLOW_URI = "http://mlflow:5000"
FEAST_HOST = "feast-online-server"
FEAST_PORT = 6566


# ==================== Helper Functions ====================

def get_redis_client() -> redis.Redis:
    """Создает клиент Redis"""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def load_model_from_mlflow(model_name: str) -> Any:
    """Загружает модель из MLflow"""
    # В реальной реализации здесь будет загрузка из MLflow
    # Для демонстрации возвращаем mock объект
    
    if model_name not in model_cache:
        # Mock загрузка модели
        model_cache[model_name] = {
            'loaded': True,
            'type': 'lstm',
            'metadata': {
                'input_size': 8,
                'sequence_length': 60
            }
        }
    
    return model_cache[model_name]


def get_features_from_feast(entity_id: str, feature_names: List[str]) -> Dict:
    """Получает признаки из Feast/Redis"""
    try:
        client = get_redis_client()
        features = {}
        
        for feature_name in feature_names:
            key = f"features:{entity_id}:{feature_name}"
            value = client.get(key)
            if value:
                features[feature_name] = float(value)
        
        return features
    except Exception as e:
        print(f"Error getting features from Redis: {e}")
        return {}


def calculate_risk_level(probability: float) -> str:
    """Определяет уровень риска по вероятности"""
    if probability < 0.3:
        return "LOW"
    elif probability < 0.6:
        return "MEDIUM"
    elif probability < 0.8:
        return "HIGH"
    else:
        return "CRITICAL"


def prepare_sequence_for_rnn(sequence_data: List[Dict], 
                             feature_names: List[str]) -> np.ndarray:
    """Подготавливает последовательность для RNN/LSTM"""
    if not sequence_data:
        return np.zeros((1, 60, len(feature_names)))
    
    # Извлечение признаков
    data = []
    for record in sequence_data:
        features = [record.get(f, 0.0) for f in feature_names]
        data.append(features)
    
    data = np.array(data)
    
    # Нормализация (в реальности нужно использовать scaler из обучения)
    mean = data.mean(axis=0)
    std = data.std(axis=0) + 1e-8
    data = (data - mean) / std
    
    # Добавление размерности batch
    if len(data.shape) == 2:
        data = data[np.newaxis, :, :]
    
    return data


async def log_inference_to_kafka(request: PredictionRequest, 
                                  response: PredictionResponse):
    """Логирует инференс в Kafka"""
    # В реальной реализации отправка в Kafka
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'model_name': request.model_name,
        'entity_id': request.entity_id,
        'prediction': response.prediction,
        'probability': response.probability
    }
    print(f"[KAFKA] Inference log: {json.dumps(log_entry)}")


# ==================== API Endpoints ====================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Проверка здоровья сервиса"""
    services = {}
    
    # Проверка Redis
    try:
        r = get_redis_client()
        r.ping()
        services['redis'] = True
    except:
        services['redis'] = False
    
    # Проверка MLflow
    try:
        resp = requests.get(MLFLOW_URI, timeout=2)
        services['mlflow'] = resp.status_code == 200
    except:
        services['mlflow'] = False
    
    all_healthy = all(services.values())
    
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        services=services,
        timestamp=datetime.now().isoformat()
    )


@app.get("/models", response_model=List[ModelInfo])
async def list_models():
    """Список доступных моделей"""
    return [
        ModelInfo(
            name="lstm_engine_failure",
            type="LSTM",
            input_size=8,
            sequence_length=60,
            feature_names=[
                "engine_temperature", "engine_rpm", "vessel_speed", "fuel_level",
                "vibration_level", "oil_pressure", "coolant_flow", "exhaust_temperature"
            ],
            version="1.0.0",
            metrics={"accuracy": 0.94, "f1_score": 0.91}
        ),
        ModelInfo(
            name="gru_temperature_forecast",
            type="GRU",
            input_size=8,
            sequence_length=30,
            feature_names=[
                "engine_temperature", "engine_rpm", "vessel_speed", "fuel_level",
                "vibration_level", "oil_pressure", "coolant_flow", "exhaust_temperature"
            ],
            version="1.0.0",
            metrics={"mae": 2.3, "rmse": 3.1}
        ),
        ModelInfo(
            name="cnn_vibration_anomaly",
            type="TemporalCNN",
            input_size=8,
            sequence_length=100,
            feature_names=[
                "engine_temperature", "engine_rpm", "vessel_speed", "fuel_level",
                "vibration_level", "oil_pressure", "coolant_flow", "exhaust_temperature"
            ],
            version="1.0.0",
            metrics={"accuracy": 0.92, "precision": 0.89}
        )
    ]


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest, background_tasks: BackgroundTasks):
    """
    Выполняет предсказание отказа двигателя
    
    - **model_name**: Название модели для использования
    - **entity_id**: ID судна
    - **request_features**: Признаки времени запроса (wave_height, throttle_position и т.д.)
    - **sequence_data**: Последовательность телеметрии для RNN/LSTM
    """
    try:
        # Загрузка модели
        model = load_model_from_mlflow(request.model_name)
        
        # Получение исторических признаков из Feast/Redis
        feature_names = model['metadata'].get('feature_names', [])
        historical_features = get_features_from_feast(request.entity_id, feature_names)
        
        # Объединение признаков
        all_features = {**historical_features, **request.request_features}
        
        # Подготовка данных для RNN/LSTM
        if request.sequence_data:
            input_data = prepare_sequence_for_rnn(
                request.sequence_data, 
                feature_names
            )
        else:
            # Если нет последовательности, используем моковые данные
            input_data = np.random.randn(1, 60, 8)
        
        # Инференс модели (mock)
        # В реальной реализации: prediction = model.predict(input_data)
        probability = np.random.uniform(0.1, 0.9)
        prediction = int(probability > 0.5)
        
        # Определение уровня риска
        risk_level = calculate_risk_level(probability)
        
        # Формирование ответа
        response = PredictionResponse(
            model_name=request.model_name,
            entity_id=request.entity_id,
            prediction=prediction,
            probability=float(probability),
            risk_level=risk_level,
            timestamp=datetime.now().isoformat(),
            features_used=list(all_features.keys()),
            metadata={
                "model_type": model['type'],
                "sequence_length": model['metadata'].get('sequence_length', 60),
                "inference_time_ms": 15.3
            }
        )
        
        # Логирование в Kafka (асинхронно)
        background_tasks.add_task(log_inference_to_kafka, request, response)
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch", response_model=List[PredictionResponse])
async def predict_batch(requests: List[PredictionRequest]):
    """Пакетное предсказание для нескольких судов"""
    results = []
    
    for req in requests:
        # Рекурсивный вызов одиночного предсказания
        result = await predict(req)
        results.append(result)
    
    return results


@app.get("/features/{entity_id}", response_model=Dict[str, float])
async def get_entity_features(entity_id: str):
    """Получение текущих признаков для судна"""
    feature_names = [
        "engine_temperature", "engine_rpm", "vessel_speed", "fuel_level",
        "vibration_level", "oil_pressure", "coolant_flow", "exhaust_temperature"
    ]
    
    features = get_features_from_feast(entity_id, feature_names)
    
    if not features:
        raise HTTPException(status_code=404, detail="Features not found")
    
    return features


@app.post("/features/{entity_id}/update")
async def update_features(entity_id: str, features: Dict[str, float]):
    """Обновление признаков в Online Store"""
    try:
        client = get_redis_client()
        
        for feature_name, value in features.items():
            key = f"features:{entity_id}:{feature_name}"
            client.set(key, str(value))
        
        return {"status": "success", "updated": len(features)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomaly/history/{entity_id}", response_model=List[Dict])
async def get_anomaly_history(entity_id: str, limit: int = 100):
    """История аномалий для судна"""
    # В реальной реализации чтение из Offline Store (Iceberg/MinIO)
    mock_history = [
        {
            "timestamp": datetime.now().isoformat(),
            "anomaly_type": "temperature_spike",
            "severity": "high",
            "duration_sec": 120
        }
    ]
    
    return mock_history[:limit]


# ==================== Startup/Shutdown Events ====================

@app.on_event("startup")
async def startup_event():
    """Инициализация при старте"""
    print("Starting Ship Feature Store API...")
    
    # Предзагрузка популярных моделей
    popular_models = ["lstm_engine_failure", "gru_temperature_forecast"]
    for model_name in popular_models:
        try:
            load_model_from_mlflow(model_name)
            print(f"Pre-loaded model: {model_name}")
        except Exception as e:
            print(f"Failed to pre-load {model_name}: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке"""
    print("Shutting down Ship Feature Store API...")
    model_cache.clear()


# ==================== Main Entry Point ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
