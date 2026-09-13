# Задание для Агента 3: ML Engineer (RNN/LSTM Models)

## 🎯 Роль
Вы отвечаете за разработку, обучение и регистрацию рекуррентных нейронных сетей (LSTM, GRU) для предсказания отказов оборудования судна на основе временных рядов телеметрии. Модели должны поддерживать работу с последовательностями и интегрироваться с MLflow.

## 📂 Расположение файлов
Все ваши файлы должны находиться в папке: `/workspace/ship-feature-store/ml/`

## 📋 Список задач

### 1. Модели временных рядов
Создайте файл `ml/models/lstm_models.py` с реализацией следующих архитектур:

#### a) Базовая LSTM модель
```python
class LSTMFailurePredictor(nn.Module):
    """
    LSTM для бинарной классификации (отказ/норма)
    Вход: последовательность длиной 60 шагов × 8 признаков
    Выход: вероятность отказа двигателя
    """
    - Input: (batch_size, seq_length=60, input_size=8)
    - LSTM слои: 2 слоя, hidden_size=128, dropout=0.3
    - Fully connected: 64 → 32 → 1 (sigmoid)
    - Output: probability of failure (0-1)
```

#### b) Bidirectional LSTM с Attention
```python
class BiLSTMAttention(nn.Module):
    """
    Двунаправленная LSTM с механизмом внимания
    Лучше улавливает контекст временного ряда
    """
    - Bidirectional LSTM: 2 слоя, hidden_size=256
    - Attention layer: вычисляет веса для каждого timestep
    - Context vector: взвешенная сумма hidden states
    - Output: probability + attention weights (для интерпретации)
```

#### c) GRU модель (альтернатива LSTM)
```python
class GRUFailurePredictor(nn.Module):
    """
    GRU — более легкая альтернатива LSTM
    Быстрее обучается, немного хуже точность
    """
    - GRU слои: 2 слоя, hidden_size=128
    - Output: probability of failure
```

#### d) CNN-LSTM гибридная модель
```python
class CNNLSTMModel(nn.Module):
    """
    CNN извлекает локальные паттерны, LSTM обрабатывает временную зависимость
    """
    - 1D Convolutional layers: 2 слоя, kernel_size=3, filters=64
    - MaxPooling: pool_size=2
    - LSTM: 1 слой, hidden_size=128
    - Output: probability of failure
```

### 2. Подготовка данных
Создайте файл `ml/data/telemetry_dataset.py`:

#### Функция создания последовательностей:
```python
def create_sequences(data: np.ndarray, labels: np.ndarray, 
                     seq_length: int = 60) -> Tuple[np.ndarray, np.ndarray]:
    """
    Преобразует плоские данные в последовательности для RNN
    
    Args:
        data: массив shape (n_samples, n_features)
        labels: целевая переменная (0/1)
        seq_length: длина последовательности (60 шагов = 1 час при 1 мин интервале)
    
    Returns:
        sequences: shape (n_sequences, seq_length, n_features)
        labels: shape (n_sequences,)
    """
```

#### Признаки для модели (8 features):
1. `engine_temperature` — температура двигателя (°C)
2. `engine_rpm` — обороты двигателя (RPM)
3. `vessel_speed` — скорость судна (узлы)
4. `fuel_level` — уровень топлива (%)
5. `vibration_level` — уровень вибрации (g)
6. `oil_pressure` — давление масла (бар)
7. `coolant_flow` — поток охладителя (л/мин)
8. `exhaust_temperature` — температура выхлопа (°C)

#### Нормализация:
- Используйте `StandardScaler` или `MinMaxScaler` из sklearn
- Сохраните scaler в MLflow как артефакт
- При инференсе применяйте тот же scaler

### 3. Обучение моделей
Создайте файл `ml/training/train.py`:

#### Функция обучения:
```python
def train_model(
    model: nn.Module,
    train_loader: DataLoader,
    val_loader: DataLoader,
    config: dict,
    mlflow_tracking_uri: str
) -> dict:
    """
    Обучает модель с логированием в MLflow
    
    Args:
        model: экземпляр модели
        train_loader: DataLoader для тренировочных данных
        val_loader: DataLoader для валидационных данных
        config: конфиг (learning_rate, batch_size, epochs, etc.)
        mlflow_tracking_uri: адрес MLflow сервера
    
    Returns:
        metrics: dict с метриками (accuracy, precision, recall, f1, auc_roc)
    """
```

#### Логирование в MLflow:
- **Параметры**: learning_rate, batch_size, seq_length, hidden_size, dropout
- **Метрики**: accuracy, precision, recall, f1_score, auc_roc (на валидации)
- **Артефакты**:
  - `model.pth` — веса модели
  - `scaler.pkl` — scaler для нормализации
  - `config.yaml` — конфигурация обучения
  - `confusion_matrix.png` — визуализация
  - `roc_curve.png` — ROC кривая

#### Early Stopping:
- Мониторьте `val_loss`
- Patience: 10 эпох
- Restore best weights: True

### 4. Скрипт переобучения (Trainer)
Создайте файл `ml/training/retrain_pipeline.py`:

#### Пайплайн автоматического переобучения:
1. **Чтение данных из Iceberg**:
   ```python
   from pyiceberg.catalog import load_catalog
   
   catalog = load_catalog("ship_catalog", warehouse="s3://iceberg-data/warehouse")
   table = catalog.load_table("ship_features.telemetry_history")
   df = table.scan().to_arrow()
   ```

2. **Загрузка лейблов**:
   - Прочитайте таблицу с лейблами (из `inference_logs` + ручная разметка)
   - Join по `entity_id` и `timestamp`

3. **Подготовка данных**:
   - Очистка от пропусков
   - Нормализация
   - Создание последовательностей

4. **Обучение новой модели**:
   - Если метрики новой модели лучше текущей → зарегистрировать в MLflow
   - Иначе → отклонить

5. **Регистрация в MLflow Model Registry**:
   ```python
   mlflow.register_model(
       model_uri=f"runs:/{run_id}/model",
       name="lstm_engine_failure",
       await_registration_for=300
   )
   ```

### 5. Детекция дрейфа данных
Создайте файл `ml/monitoring/drift_detector.py`:

#### Использование Evidently AI:
```python
from evidently.report import Report
from evidently.metrics import DataDriftTable

def detect_drift(reference_data: pd.DataFrame, current_data: pd.DataFrame):
    """
    Сравнивает распределение текущих данных с референсными
    
    Returns:
        drift_detected: bool
        drift_report: dict с метриками дрейфа
    """
```

#### Метрики для проверки:
- PSI (Population Stability Index) для каждого признака
- Распределение (Kolmogorov-Smirnov test)
- Статистики (mean, std, median)

#### Действия при обнаружении дрейфа:
- Логирование в Kafka топик `drift_alerts`
- Триггер переобучения модели
- Уведомление (email/slack webhook)

### 6. Конфигурация
Создайте файл `ml/config/models_config.yaml`:
```yaml
models:
  lstm_engine_failure:
    type: lstm
    input_size: 8
    hidden_size: 128
    num_layers: 2
    seq_length: 60
    dropout: 0.3
    learning_rate: 0.001
    batch_size: 32
    epochs: 50
    
  gruf_engine_failure:
    type: gru
    hidden_size: 128
    ...

training:
  early_stopping_patience: 10
  restore_best_weights: true
  validation_split: 0.2
  
retraining:
  schedule: "0 2 * * *"  # Cron: каждый день в 2 AM
  min_samples: 10000
  performance_threshold: 0.05  # Улучшение на 5%
```

### 7. Requirements
Создайте файл `ml/requirements.txt`:
```
torch>=2.0.0
torchvision>=0.15.0
scikit-learn>=1.3.0
pandas>=2.0.0
numpy>=1.24.0
mlflow>=2.8.0
pyiceberg>=0.5.0
evidently>=0.4.0
kafka-python>=2.0.0
pyyaml>=6.0
```

## 🔗 Точки интеграции
- **Входные данные**: Iceberg таблица `s3://iceberg-data/warehouse/ship_features/telemetry` (от Flink)
- **Лейблы**: Kafka топик `inference_logs` + таблица labels в Iceberg
- **Выход**: MLflow Model Registry (модели доступны API серверу)
- **Мониторинг**: Kafka топик `drift_alerts` (для уведомлений)

## ✅ Критерии приемки
1. Все 4 типа моделей (LSTM, BiLSTM+Attention, GRU, CNN-LSTM) обучаются без ошибок
2. Модель достигает AUC-ROC >= 0.85 на валидационной выборке
3. Последовательности создаются корректно (shape: [batch, 60, 8])
4. MLflow логирует параметры, метрики и артефакты
5. Trainer успешно читает данные из Iceberg и обучает новую модель
6. Drift detector обнаруживает искусственный дрейф (тест с измененным распределением)
7. Модели регистрируются в MLflow Model Registry с версиями

## 🛠️ Технические требования
- Используйте PyTorch 2.0+ (или TensorFlow 2.x, если предпочитаете)
- Код должен быть модульным (отдельные классы для моделей, dataset, training)
- Поддержка GPU через CUDA (если доступен)
- Воспроизводимость экспериментов (seed, deterministic mode)

## 📝 Примечания
- Для интерпретируемости сохраняйте attention weights
- Предусмотрите возможность дообучения (fine-tuning) существующих моделей
- Добавьте unit тесты для функций создания последовательностей
