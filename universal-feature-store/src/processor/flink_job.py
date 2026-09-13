"""
Apache Flink Job for Stream Processing of Vessel Telemetry
Computes windowed aggregates and updates features in real-time
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.connector.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
import json
from datetime import datetime
from typing import Dict, List


def create_flink_table_environment() -> TableEnvironment:
    """Создает окружение Flink Table API"""
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    
    # Настройка чекпоинтов
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "10000"
    )
    table_env.get_config().get_configuration().set_string(
        "state.backend", "rocksdb"
    )
    
    return table_env


def register_kafka_source(table_env: TableEnvironment, 
                          kafka_broker: str, 
                          topic: str):
    """Регистрирует Kafka источник"""
    
    table_env.execute_sql(f"""
        CREATE TABLE vessel_events (
            entity_id STRING,
            timestamp STRING,
            event_type STRING,
            metrics ROW<
                engine_temperature DOUBLE,
                engine_rpm DOUBLE,
                vessel_speed DOUBLE,
                fuel_level DOUBLE,
                vibration_level DOUBLE,
                oil_pressure DOUBLE,
                coolant_flow DOUBLE,
                exhaust_temperature DOUBLE
            >,
            status ROW<
                engine_status STRING,
                anomaly_detected BOOLEAN,
                anomaly_type STRING
            >,
            environmental ROW<
                wave_height DOUBLE,
                wind_speed DOUBLE,
                water_temperature DOUBLE
            >,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'topic' = '{topic}',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    
    print(f"Registered Kafka source: {topic}")


def register_redis_sink(table_env: TableEnvironment, 
                        redis_host: str, 
                        redis_port: int):
    """Регистрирует Redis сток для Online Store"""
    
    # В реальной реализации используется кастомный sink для Redis
    # Для демонстрации используем print sink
    table_env.execute_sql(f"""
        CREATE TABLE redis_features (
            entity_id STRING,
            feature_name STRING,
            feature_value DOUBLE,
            timestamp TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )
    """)
    
    print(f"Registered Redis sink (mock): {redis_host}:{redis_port}")


def register_iceberg_sink(table_env: TableEnvironment,
                          minio_endpoint: str,
                          bucket: str):
    """Регистрирует Iceberg сток для Offline Store"""
    
    table_env.execute_sql(f"""
        CREATE TABLE iceberg_telemetry (
            entity_id STRING,
            timestamp TIMESTAMP(3),
            engine_temperature DOUBLE,
            engine_rpm DOUBLE,
            vessel_speed DOUBLE,
            fuel_level DOUBLE,
            vibration_level DOUBLE,
            oil_pressure DOUBLE,
            coolant_flow DOUBLE,
            exhaust_temperature DOUBLE,
            anomaly_detected BOOLEAN,
            anomaly_type STRING
        ) WITH (
            'connector' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = 's3a://{bucket}/iceberg',
            'format-version' = '2'
        )
    """)
    
    print(f"Registered Iceberg sink: {bucket}")


def compute_windowed_aggregates(table_env: TableEnvironment):
    """Вычисляет оконные агрегаты"""
    
    # Tumbling окно 1 час для средних значений
    table_env.execute_sql("""
        INSERT INTO redis_features
        SELECT 
            entity_id,
            'avg_engine_temp_1h' as feature_name,
            AVG(metrics.engine_temperature) as feature_value,
            TUMBLE_END(event_time, INTERVAL '1' HOUR) as timestamp
        FROM vessel_events
        GROUP BY entity_id, TUMBLE(event_time, INTERVAL '1' HOUR)
    """)
    
    # Hopping окно 10 минут для стандартного отклонения
    table_env.execute_sql("""
        INSERT INTO redis_features
        SELECT 
            entity_id,
            'std_vibration_10min' as feature_name,
            STDDEV_POP(metrics.vibration_level) as feature_value,
            HOP_END(event_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) as timestamp
        FROM vessel_events
        GROUP BY entity_id, HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)
    """)
    
    # Session окно для обнаружения аномалий
    table_env.execute_sql("""
        INSERT INTO redis_features
        SELECT 
            entity_id,
            'max_temp_session' as feature_name,
            MAX(metrics.engine_temperature) as feature_value,
            SESSION_END(event_time, INTERVAL '30' MINUTE) as timestamp
        FROM vessel_events
        WHERE status.anomaly_detected = TRUE
        GROUP BY entity_id, SESSION(event_time, INTERVAL '30' MINUTE)
    """)
    
    print("Registered windowed aggregate queries")


def process_telemetry_stream(kafka_broker: str = "kafka:29092",
                             redis_host: str = "redis",
                             redis_port: int = 6379,
                             minio_endpoint: str = "minio:9000"):
    """Основная функция обработки телеметрии"""
    
    print("=" * 50)
    print("Starting Flink Telemetry Processing Job")
    print("=" * 50)
    
    # Создание окружения
    table_env = create_flink_table_environment()
    
    # Регистрация источников и стоков
    register_kafka_source(table_env, kafka_broker, "vessel_events")
    register_redis_sink(table_env, redis_host, redis_port)
    register_iceberg_sink(table_env, minio_endpoint, "vessel-data")
    
    # Вычисление агрегатов
    compute_windowed_aggregates(table_env)
    
    # Запись в Iceberg для исторического хранения
    table_env.execute_sql("""
        INSERT INTO iceberg_telemetry
        SELECT 
            entity_id,
            event_time,
            metrics.engine_temperature,
            metrics.engine_rpm,
            metrics.vessel_speed,
            metrics.fuel_level,
            metrics.vibration_level,
            metrics.oil_pressure,
            metrics.coolant_flow,
            metrics.exhaust_temperature,
            status.anomaly_detected,
            status.anomaly_type
        FROM vessel_events
    """)
    
    print("Job submitted successfully")
    print("Waiting for data...")
    
    # Выполнение job
    table_env.execute("Ship Telemetry Processing Job")


class FlinkJobMock:
    """Mock класс для тестирования без полноценного Flink кластера"""
    
    def __init__(self, kafka_broker: str, redis_host: str):
        self.kafka_broker = kafka_broker
        self.redis_host = redis_host
        self.window_size = 60  # секунд
        self.buffer: Dict[str, List[Dict]] = {}
    
    def process_message(self, message: Dict):
        """Обрабатывает одно сообщение"""
        entity_id = message.get('entity_id', 'unknown')
        
        if entity_id not in self.buffer:
            self.buffer[entity_id] = []
        
        self.buffer[entity_id].append(message)
        
        # Очистка буфера по размеру окна
        if len(self.buffer[entity_id]) > self.window_size:
            self.buffer[entity_id] = self.buffer[entity_id][-self.window_size:]
        
        # Вычисление агрегатов
        aggregates = self._compute_aggregates(entity_id)
        
        return aggregates
    
    def _compute_aggregates(self, entity_id: str) -> Dict:
        """Вычисляет агрегаты для судна"""
        messages = self.buffer.get(entity_id, [])
        
        if not messages:
            return {}
        
        temps = [m['metrics']['engine_temperature'] for m in messages]
        rpms = [m['metrics']['engine_rpm'] for m in messages]
        vibrations = [m['metrics']['vibration_level'] for m in messages]
        
        aggregates = {
            'entity_id': entity_id,
            'timestamp': datetime.now().isoformat(),
            'avg_engine_temp_1h': sum(temps) / len(temps),
            'std_engine_temp_1h': (sum((t - sum(temps)/len(temps))**2 for t in temps) / len(temps)) ** 0.5,
            'avg_rpm_1h': sum(rpms) / len(rpms),
            'max_vibration_1h': max(vibrations),
            'anomaly_count': sum(1 for m in messages if m['status'].get('anomaly_detected', False))
        }
        
        print(f"[FLINK] Aggregates for {entity_id}: {aggregates}")
        return aggregates


def main():
    """Точка входа для запуска Flink job"""
    import os
    
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    redis_host = os.getenv("REDIS_HOST", "redis")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    
    try:
        # Попытка запустить полноценный Flink job
        process_telemetry_stream(
            kafka_broker=kafka_broker,
            redis_host=redis_host,
            minio_endpoint=minio_endpoint
        )
    except Exception as e:
        print(f"Flink cluster not available, running mock: {e}")
        
        # Mock режим для тестирования
        mock_job = FlinkJobMock(kafka_broker, redis_host)
        
        # Тестовые данные
        from ml.telemetry_generator import VesselTelemetryGenerator
        
        generator = VesselTelemetryGenerator()
        
        for i in range(100):
            telemetry = generator.generate_telemetry()
            aggregates = mock_job.process_message(telemetry)
            
            if i % 10 == 0:
                print(f"Processed {i} messages")


if __name__ == "__main__":
    main()
