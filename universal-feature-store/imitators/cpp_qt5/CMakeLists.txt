cmake_minimum_required(VERSION 3.10)
project(ShipTelemetrySimulator VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

# Поиск Qt5
find_package(Qt5 COMPONENTS Core Network Charts REQUIRED)

# Поиск зависимостей
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED librdkafka)
pkg_check_modules(SPDLOG REQUIRED spdlog)

# Пути включения
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${RDKAFKA_INCLUDE_DIRS}
    ${SPDLOG_INCLUDE_DIRS}
)

# Исходный код демона
set(DAEMON_SOURCES
    src/main_daemon.cpp
    src/telemetry_generator.cpp
    src/kafka_producer.cpp
)

# Исходный код GUI
set(GUI_SOURCES
    src/main_gui.cpp
    src/control_panel.cpp
    src/telemetry_generator.cpp
    src/kafka_producer.cpp
)

# Заголовочные файлы
set(HEADERS
    include/telemetry_generator.h
    include/kafka_producer.h
    include/control_panel.h
)

# Создание исполняемого файла демона
add_executable(ship_telemetry_daemon ${DAEMON_SOURCES} ${HEADERS})
target_link_libraries(ship_telemetry_daemon
    Qt5::Core
    Qt5::Network
    ${RDKAFKA_LIBRARIES}
    ${SPDLOG_LIBRARIES}
)

# Создание исполняемого файла GUI
add_executable(ship_control_panel ${GUI_SOURCES} ${HEADERS})
target_link_libraries(ship_control_panel
    Qt5::Core
    Qt5::Network
    Qt5::Charts
    ${RDKAFKA_LIBRARIES}
    ${SPDLOG_LIBRARIES}
)

# Установка флагов компиляции
target_compile_options(ship_telemetry_daemon PRIVATE ${RDKAFKA_CFLAGS_OTHER})
target_compile_options(ship_control_panel PRIVATE ${RDKAFKA_CFLAGS_OTHER})

# Директории для установки
install(TARGETS ship_telemetry_daemon ship_control_panel
    RUNTIME DESTINATION bin
)

# Копирование конфигурационного файла
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/config.json.example
    ${CMAKE_CURRENT_BINARY_DIR}/config.json
    COPYONLY
)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Qt5 version: ${Qt5Core_VERSION}")
message(STATUS "librdkafka version: ${RDKAFKA_VERSION}")
