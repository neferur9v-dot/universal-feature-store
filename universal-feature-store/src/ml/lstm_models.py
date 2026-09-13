"""
LSTM/RNN Models for Vessel Engine Failure Prediction
Time Series Forecasting with Recurrent Neural Networks
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from typing import Dict, List, Tuple, Optional
import json


class VesselTelemetryDataset(Dataset):
    """Dataset для временных рядов телеметрии судна"""
    
    def __init__(self, sequences: np.ndarray, labels: np.ndarray, 
                 sequence_length: int = 60):
        """
        Args:
            sequences: Массив данных формы (samples, timesteps, features)
            labels: Метки для каждого сэмпла
            sequence_length: Длина последовательности
        """
        self.sequences = torch.FloatTensor(sequences)
        self.labels = torch.FloatTensor(labels)
        self.sequence_length = sequence_length
    
    def __len__(self) -> int:
        return len(self.sequences)
    
    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:
        return self.sequences[idx], self.labels[idx]


class LSTMFailurePredictor(nn.Module):
    """LSTM модель для предсказания отказа двигателя"""
    
    def __init__(self, input_size: int = 8, hidden_size: int = 128, 
                 num_layers: int = 2, dropout: float = 0.3,
                 bidirectional: bool = True):
        """
        Args:
            input_size: Количество входных признаков
            hidden_size: Размер скрытого слоя
            num_layers: Количество LSTM слоев
            dropout: Dropout rate
            bidirectional: Использовать ли Bidirectional LSTM
        """
        super(LSTMFailurePredictor, self).__init__()
        
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.num_directions = 2 if bidirectional else 1
        
        # LSTM слои
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0,
            bidirectional=bidirectional
        )
        
        # Attention механизм
        self.attention = nn.Sequential(
            nn.Linear(hidden_size * self.num_directions, 64),
            nn.Tanh(),
            nn.Linear(64, 1)
        )
        
        # Полносвязные слои
        self.fc = nn.Sequential(
            nn.Linear(hidden_size * self.num_directions, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Forward pass
        
        Args:
            x: Входные данные формы (batch, seq_len, input_size)
        
        Returns:
            Предсказание вероятности отказа
        """
        # LSTM forward
        lstm_out, (h_n, c_n) = self.lstm(x)
        
        # Attention mechanism
        attention_weights = self.attention(lstm_out)
        attention_weights = torch.softmax(attention_weights, dim=1)
        
        # Weighted sum of LSTM outputs
        context = torch.sum(attention_weights * lstm_out, dim=1)
        
        # Fully connected layers
        output = self.fc(context)
        
        return output.squeeze(-1)
    
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        """Предсказание вероятности для numpy массива"""
        self.eval()
        with torch.no_grad():
            x_tensor = torch.FloatTensor(x)
            if len(x_tensor.shape) == 2:
                x_tensor = x_tensor.unsqueeze(0)
            proba = self.forward(x_tensor).numpy()
            return proba
    
    def predict(self, x: np.ndarray, threshold: float = 0.5) -> np.ndarray:
        """Бинарное предсказание"""
        proba = self.predict_proba(x)
        return (proba > threshold).astype(int)


class GRUFailurePredictor(nn.Module):
    """GRU модель для предсказания отказа двигателя (более легкая альтернатива)"""
    
    def __init__(self, input_size: int = 8, hidden_size: int = 64, 
                 num_layers: int = 2, dropout: float = 0.3):
        super(GRUFailurePredictor, self).__init__()
        
        self.gru = nn.GRU(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )
        
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        gru_out, h_n = self.gru(x)
        output = self.fc(h_n[-1])
        return output.squeeze(-1)


class TemporalCNN(nn.Module):
    """1D CNN для обработки временных рядов"""
    
    def __init__(self, input_size: int = 8, num_classes: int = 1):
        super(TemporalCNN, self).__init__()
        
        self.conv1 = nn.Conv1d(input_size, 64, kernel_size=3, padding=1)
        self.conv2 = nn.Conv1d(64, 128, kernel_size=3, padding=1)
        self.conv3 = nn.Conv1d(128, 128, kernel_size=3, padding=1)
        
        self.pool = nn.MaxPool1d(2)
        self.relu = nn.ReLU()
        self.dropout = nn.Dropout(0.3)
        
        self.fc = nn.Sequential(
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x shape: (batch, seq_len, features) -> (batch, features, seq_len)
        x = x.permute(0, 2, 1)
        
        x = self.relu(self.conv1(x))
        x = self.pool(x)
        x = self.dropout(x)
        
        x = self.relu(self.conv2(x))
        x = self.pool(x)
        x = self.dropout(x)
        
        x = self.relu(self.conv3(x))
        x = self.pool(x)
        
        # Global average pooling
        x = x.mean(dim=2)
        
        output = self.fc(x)
        return output.squeeze(-1)


class EnsembleModel(nn.Module):
    """Ансамбль моделей для улучшения точности"""
    
    def __init__(self, models: List[nn.Module], weights: Optional[List[float]] = None):
        super(EnsembleModel, self).__init__()
        self.models = nn.ModuleList(models)
        self.weights = weights or [1.0 / len(models)] * len(models)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        outputs = []
        for model, weight in zip(self.models, self.weights):
            out = model(x)
            outputs.append(out * weight)
        return torch.sum(torch.stack(outputs), dim=0)


def create_sequences(data: np.ndarray, labels: np.ndarray, 
                     sequence_length: int = 60, 
                     stride: int = 1) -> Tuple[np.ndarray, np.ndarray]:
    """
    Создает последовательности из временных рядов
    
    Args:
        data: Исходные данные формы (timesteps, features)
        labels: Метки для каждого timestep
        sequence_length: Длина каждой последовательности
        stride: Шаг между последовательностями
    
    Returns:
        sequences, labels для обучения
    """
    sequences = []
    seq_labels = []
    
    for i in range(0, len(data) - sequence_length, stride):
        seq = data[i:i + sequence_length]
        # Берем метку последнего элемента последовательности
        label = labels[i + sequence_length - 1]
        sequences.append(seq)
        seq_labels.append(label)
    
    return np.array(sequences), np.array(seq_labels)


def prepare_telemetry_data(telemetry_records: List[Dict], 
                           sequence_length: int = 60) -> Tuple[np.ndarray, np.ndarray]:
    """
    Подготавливает данные телеметрии для RNN/LSTM
    
    Args:
        telemetry_records: Список записей телеметрии
        sequence_length: Длина последовательности
    
    Returns:
        X: Последовательности формы (samples, seq_len, features)
        y: Метки
    """
    feature_names = [
        'engine_temperature', 'engine_rpm', 'vessel_speed', 'fuel_level',
        'vibration_level', 'oil_pressure', 'coolant_flow', 'exhaust_temperature'
    ]
    
    # Извлечение признаков
    data = []
    labels = []
    
    for record in telemetry_records:
        metrics = record.get('metrics', {})
        features = [metrics.get(f, 0.0) for f in feature_names]
        data.append(features)
        
        # Метка: 1 если обнаружена аномалия, иначе 0
        status = record.get('status', {})
        label = 1.0 if status.get('anomaly_detected', False) else 0.0
        labels.append(label)
    
    data = np.array(data)
    labels = np.array(labels)
    
    # Нормализация признаков
    mean = data.mean(axis=0)
    std = data.std(axis=0) + 1e-8
    data = (data - mean) / std
    
    # Создание последовательностей
    X, y = create_sequences(data, labels, sequence_length)
    
    return X, y


def train_model(model: nn.Module, train_loader: DataLoader, 
                val_loader: DataLoader = None,
                epochs: int = 50, lr: float = 0.001,
                device: str = 'cpu') -> Dict:
    """
    Обучает модель
    
    Returns:
        История обучения
    """
    model = model.to(device)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=5
    )
    
    history = {
        'train_loss': [],
        'val_loss': [],
        'val_accuracy': []
    }
    
    for epoch in range(epochs):
        # Training
        model.train()
        train_loss = 0.0
        
        for batch_X, batch_y in train_loader:
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device)
            
            optimizer.zero_grad()
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item()
        
        train_loss /= len(train_loader)
        history['train_loss'].append(train_loss)
        
        # Validation
        if val_loader:
            model.eval()
            val_loss = 0.0
            correct = 0
            total = 0
            
            with torch.no_grad():
                for batch_X, batch_y in val_loader:
                    batch_X = batch_X.to(device)
                    batch_y = batch_y.to(device)
                    
                    outputs = model(batch_X)
                    loss = criterion(outputs, batch_y)
                    val_loss += loss.item()
                    
                    predictions = (outputs > 0.5).float()
                    correct += (predictions == batch_y).sum().item()
                    total += batch_y.size(0)
            
            val_loss /= len(val_loader)
            val_acc = correct / total if total > 0 else 0
            
            history['val_loss'].append(val_loss)
            history['val_accuracy'].append(val_acc)
            
            scheduler.step(val_loss)
            
            print(f"Epoch {epoch+1}/{epochs} - "
                  f"Train Loss: {train_loss:.4f}, "
                  f"Val Loss: {val_loss:.4f}, "
                  f"Val Acc: {val_acc:.4f}")
    
    return history


def save_model(model: nn.Module, path: str, metadata: Dict = None):
    """Сохраняет модель и метаданные"""
    torch.save({
        'model_state_dict': model.state_dict(),
        'metadata': metadata or {}
    }, path)
    print(f"Model saved to {path}")


def load_model(model_class, path: str, **model_kwargs) -> nn.Module:
    """Загружает модель из файла"""
    checkpoint = torch.load(path)
    model = model_class(**model_kwargs)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    print(f"Model loaded from {path}")
    return model


# Пример использования
if __name__ == "__main__":
    # Генерация тестовых данных
    from telemetry_generator import VesselTelemetryGenerator
    
    generator = VesselTelemetryGenerator()
    records = generator.generate_sequence(1000)
    
    # Подготовка данных
    X, y = prepare_telemetry_data(records, sequence_length=60)
    print(f"Data shape: X={X.shape}, y={y.shape}")
    
    # Разделение на train/val
    split_idx = int(len(X) * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]
    
    # Создание DataLoader
    train_dataset = VesselTelemetryDataset(X_train, y_train)
    val_dataset = VesselTelemetryDataset(X_val, y_val)
    
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=32)
    
    # Создание и обучение модели
    model = LSTMFailurePredictor(
        input_size=8,
        hidden_size=128,
        num_layers=2,
        dropout=0.3,
        bidirectional=True
    )
    
    print(f"Model parameters: {sum(p.numel() for p in model.parameters()):,}")
    
    history = train_model(
        model, train_loader, val_loader,
        epochs=20, lr=0.001, device='cpu'
    )
    
    # Сохранение модели
    save_model(model, 'lstm_engine_failure.pth', metadata={
        'input_size': 8,
        'sequence_length': 60,
        'feature_names': [
            'engine_temperature', 'engine_rpm', 'vessel_speed', 'fuel_level',
            'vibration_level', 'oil_pressure', 'coolant_flow', 'exhaust_temperature'
        ]
    })
