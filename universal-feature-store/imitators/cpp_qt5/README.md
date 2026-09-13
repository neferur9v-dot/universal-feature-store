# Ship Telemetry Simulator - C++ Qt5

## Структура проекта

```
cpp_qt5/
├── src/
│   ├── main_daemon.cpp      # Точка входа для консольного демона
│   ├── main_gui.cpp         # Точка входа для GUI приложения
│   ├── telemetry_generator.cpp  # Генератор телеметрии
│   ├── kafka_producer.cpp   # Kafka продюсер
│   └── control_panel.cpp    # GUI панель управления
├── include/
│   ├── telemetry_generator.h
│   ├── kafka_producer.h
│   └── control_panel.h
├── CMakeLists.txt
└── Dockerfile.daemon
```

## Сборка

### Требования
- GCC >= 4.8 или Clang
- CMake >= 3.10
- Qt5 Core, Network, Charts
- librdkafka-dev, libspdlog-dev

### Команды сборки

```bash
cd imitators/cpp_qt5
mkdir build && cd build
cmake ..
make -j4

# Запуск демона
export KAFKA_BROKER="localhost:9092"
./ship_telemetry_daemon

# Запуск GUI
./ship_control_panel
```

## Описание компонентов

### Ship Telemetry Daemon (Консольный демон)

Генерирует поток событий телеметрии и отправляет в Kafka.

**Алгоритм работы:**
1. Инициализация: чтение конфига, подключение к Kafka
2. Генерация цикла: бесконечный цикл с интервалом 1 секунда
3. Синтез данных:
   - Генерация entity_id (ID судна)
   - Создание случайных, но реалистичных значений телеметрии
   - Добавление шума и редких аномалий
   - Формирование временной метки
4. Сериализация в JSON
5. Отправка в Kafka топик `vessel_events`
6. Логирование статуса

### Ship Control Panel (GUI приложение)

Интерфейс для ручного управления параметрами и запроса предсказаний.

**Возможности:**
- Вкладка "Настройки": настройка подключения к Kafka и API
- Вкладка "Параметры судна": слайдеры для изменения request-time features
- Вкладка "Мониторинг": графики в реальном времени с ответами модели
- Вкладка "Логи": история запросов и предсказаний

**Алгоритм работы:**
1. Пользователь выбирает модель из списка
2. Устанавливает параметры через слайдеры
3. Нажимает кнопку "Запросить предсказание"
4. Приложение отправляет HTTP POST на `/predict`
5. Получает ответ и отображает результат

## Docker сборка

```bash
# Сборка образа демона
docker build -f Dockerfile.daemon -t ship-telemetry-daemon .

# Запуск
docker run -e KAFKA_BROKER=kafka:29092 ship-telemetry-daemon
```
