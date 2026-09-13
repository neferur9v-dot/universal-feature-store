"""
Universal Telemetry Data Generator with Time Series and Anomalies
Generates realistic equipment telemetry data for LSTM/RNN model training
"""

import json
import random
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class UniversalTelemetryGenerator:
    """Генератор телеметрии оборудования с временными рядами и аномалиями"""
    
    def __init__(self, entity_id: str = "ENTITY-001"):
        self.entity_id = entity_id
        self.base_time = datetime.now()
        
        # Базовые параметры оборудования
        self.base_temp_primary = 75.0  # Базовая температура (°C)
        self.base_rpm_main = 1200   # Базовые обороты
        self.base_speed_current = 12.5  # Базовая скорость
        self.base_fuel_level = 85.0   # Базовый уровень топлива (%)
        
        # Параметры для временных рядов
        self.time_step = 0
        self.temp_trend = 0.0
        self.rpm_trend = 0.0
        
        # Состояния аномалий
        self.anomaly_active = False
        self.anomaly_type: Optional[str] = None
        self.anomaly_start_time: Optional[datetime] = None
        self.anomaly_duration = 0
        
    def _generate_temporal_pattern(self, base_value: float, 
                                    amplitude: float, 
                                    period: int = 60) -> float:
        """Генерирует временную паттерн с синусоидальной компонентой"""
        seasonal = amplitude * math.sin(2 * math.pi * self.time_step / period)
        trend = 0.001 * self.time_step  # Медленный дрейф
        noise = random.gauss(0, amplitude * 0.3)
        return base_value + seasonal + trend + noise
    
    def _generate_anomaly(self, value: float, anomaly_type: str) -> float:
        """Генерирует аномальное значение"""
        if anomaly_type == "temperature_spike":
            return value + random.uniform(15, 30)
        elif anomaly_type == "rpm_drop":
            return value * random.uniform(0.5, 0.7)
        elif anomaly_type == "pressure_loss":
            return value - random.uniform(5, 15)
        elif anomaly_type == "vibration":
            return value + random.uniform(10, 20)
        return value
    
    def _check_anomaly_trigger(self) -> Optional[str]:
        """Проверяет, должна ли начаться аномалия"""
        if self.anomaly_active:
            return None
            
        # 2% шанс начала аномалии
        if random.random() < 0.02:
            anomaly_types = ["temperature_spike", "rpm_drop", "pressure_loss", "vibration"]
            selected = random.choice(anomaly_types)
            self.anomaly_active = True
            self.anomaly_type = selected
            self.anomaly_start_time = datetime.now()
            self.anomaly_duration = random.randint(5, 20)  # 5-20 шагов
            return selected
        return None
    
    def _update_anomaly_state(self):
        """Обновляет состояние аномалии"""
        if not self.anomaly_active:
            return
            
        self.anomaly_duration -= 1
        if self.anomaly_duration <= 0:
            self.anomaly_active = False
            self.anomaly_type = None
    
    def generate_telemetry(self) -> Dict:
        """Генерирует одно сообщение телеметрии"""
        self.time_step += 1
        timestamp = self.base_time + timedelta(seconds=self.time_step)
        
        # Проверка на начало аномалии
        self._check_anomaly_trigger()
        
        # Генерация основных показателей с временными паттернами
        temperature_primary = self._generate_temporal_pattern(
            self.base_temp_primary, amplitude=5, period=100
        )
        rpm_main = self._generate_temporal_pattern(
            self.base_rpm_main, amplitude=50, period=150
        )
        speed_current = self._generate_temporal_pattern(
            self.base_speed_current, amplitude=1.5, period=200
        )
        fuel_level = max(0, self.base_fuel_level - 0.01 * self.time_step + random.gauss(0, 0.5))
        
        # Применение аномалий
        if self.anomaly_active and self.anomaly_type:
            if self.anomaly_type == "temperature_spike":
                temperature_primary = self._generate_anomaly(temperature_primary, "temperature_spike")
            elif self.anomaly_type == "rpm_drop":
                rpm_main = self._generate_anomaly(rpm_main, "rpm_drop")
            elif self.anomaly_type == "pressure_loss":
                fuel_level = self._generate_anomaly(fuel_level, "pressure_loss")
        
        # Дополнительные метрики для RNN/LSTM
        vibration_index = abs(random.gauss(2.5, 0.8))
        pressure_oil = 4.5 + random.gauss(0, 0.3)
        flow_coolant = 85.0 + random.gauss(0, 3)
        temperature_exhaust = 350 + random.gauss(0, 15)
        
        # Обновление состояния аномалии
        self._update_anomaly_state()
        
        # Формирование сообщения
        telemetry = {
            "entity_id": self.entity_id,
            "timestamp": timestamp.isoformat(),
            "event_type": "telemetry_update",
            "metrics": {
                "temperature_primary": round(temperature_primary, 2),
                "rpm_main": round(rpm_main, 1),
                "speed_current": round(speed_current, 2),
                "fuel_level": round(fuel_level, 2),
                "vibration_index": round(vibration_index, 3),
                "pressure_oil": round(pressure_oil, 2),
                "flow_coolant": round(flow_coolant, 1),
                "temperature_exhaust": round(temperature_exhaust, 1)
            },
            "status": {
                "equipment_status": "normal" if not self.anomaly_active else "warning",
                "anomaly_detected": self.anomaly_active,
                "anomaly_type": self.anomaly_type
            },
            "environmental": {
                "ambient_temperature": round(random.uniform(15, 35), 1),
                "humidity": round(random.uniform(30, 80), 1),
                "load_factor": round(random.uniform(0.5, 1.0), 2)
            }
        }
        
        return telemetry
    
    def generate_sequence(self, sequence_length: int) -> List[Dict]:
        """Генерирует последовательность для RNN/LSTM"""
        return [self.generate_telemetry() for _ in range(sequence_length)]
    
    def reset(self):
        """Сбрасывает генератор в начальное состояние"""
        self.time_step = 0
        self.base_time = datetime.now()
        self.anomaly_active = False
        self.anomaly_type = None


class KafkaProducerMock:
    """Мок Kafka продюсера для тестирования"""
    
    def __init__(self, broker: str, topic: str):
        self.broker = broker
        self.topic = topic
        self.messages_sent = 0
    
    def send(self, message: Dict) -> bool:
        """Отправляет сообщение (мок)"""
        self.messages_sent += 1
        print(f"[KAFKA] Sent to {self.topic}: {json.dumps(message, indent=2)}")
        return True
    
    def send_batch(self, messages: List[Dict]) -> int:
        """Отправляет пакет сообщений"""
        for msg in messages:
            self.send(msg)
        return len(messages)


def main():
    """Основная функция демона"""
    import os
    import time
    
    # Конфигурация из переменных окружения
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "entity_events")
    entity_id = os.getenv("ENTITY_ID", "ENTITY-001")
    interval_sec = int(os.getenv("TELEMETRY_INTERVAL", "1"))
    
    print(f"=== Universal Telemetry Daemon ===")
    print(f"Kafka Broker: {kafka_broker}")
    print(f"Kafka Topic: {kafka_topic}")
    print(f"Entity ID: {entity_id}")
    print(f"Interval: {interval_sec}s")
    print("=" * 40)
    
    # Инициализация генератора и продюсера
    generator = UniversalTelemetryGenerator(entity_id)
    producer = KafkaProducerMock(kafka_broker, kafka_topic)
    
    try:
        while True:
            # Генерация телеметрии
            telemetry = generator.generate_telemetry()
            
            # Отправка в Kafka
            producer.send(telemetry)
            
            # Логирование статуса
            status = "ANOMALY" if telemetry["status"]["anomaly_detected"] else "NORMAL"
            temp = telemetry["metrics"]["temperature_primary"]
            rpm = telemetry["metrics"]["rpm_main"]
            print(f"[{status}] Temp: {temp}°C, RPM: {rpm}, Fuel: {telemetry['metrics']['fuel_level']}%")
            
            # Интервал
            time.sleep(interval_sec)
            
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")
        print(f"Total messages sent: {producer.messages_sent}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
