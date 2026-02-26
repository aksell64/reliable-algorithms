#!/usr/bin/env bash
set -e

# ========================
# Конфигурация
# ========================
TOTAL_NODES=${1:-3}           # Количество нод (по умолчанию 3)
BASE_PORT=9000                # Начальный порт
BINARY="."           # Путь к скомпилированному бинарнику
LOG_DIR="./logs"              # Папка для логов
PIDS_FILE=".node_pids"        # Файл для хранения PID-процессов

# ========================
# Сборка проекта
# ========================
echo "🔨 Собираю проект..."
go build -o "$BINARY" .
echo "✅ Сборка завершена."

# ========================
# Подготовка
# ========================
mkdir -p "$LOG_DIR"
> "$PIDS_FILE"   # очищаем файл с пидами

# Убиваем старые процессы, если остались
cleanup() {
    echo ""
    echo "🛑 Останавливаю все ноды..."
    while read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null && echo "   Убит процесс PID=$pid"
        fi
    done < "$PIDS_FILE"
    rm -f "$PIDS_FILE"
    echo "✅ Все ноды остановлены."
    exit 0
}

# По Ctrl+C — красивый shutdown всех нод
trap cleanup SIGINT SIGTERM

# ========================
# Запуск нод
# ========================
echo ""
echo "🚀 Запускаю $TOTAL_NODES нод (порты $BASE_PORT–$((BASE_PORT + TOTAL_NODES - 1)))..."
echo "================================================"

for i in $(seq 1 "$TOTAL_NODES"); do
    PORT=$((BASE_PORT + i - 1))
    ADDR="/ip4/0.0.0.0/tcp/$PORT"
    LOG_FILE="$LOG_DIR/node_${i}.log"

    $BINARY --id "$i" --n "$TOTAL_NODES" --addr "$ADDR" > "$LOG_FILE" 2>&1 &
    NODE_PID=$!
    echo "$NODE_PID" >> "$PIDS_FILE"

    echo "   Нода #$i | PID=$NODE_PID | Адрес=$ADDR | Лог=$LOG_FILE"
done

echo "================================================"
echo ""
echo "📋 Логи в папке: $LOG_DIR/"
echo "   Смотреть все логи:    tail -f $LOG_DIR/node_*.log"
echo "   Смотреть конкретную:  tail -f $LOG_DIR/node_1.log"
echo ""
echo "⏳ Все ноды работают в фоне. Нажми Ctrl+C чтобы убить всё."
echo ""

# ========================
# Стримим логи всех нод в терминал (с префиксом)
# ========================
tail -f "$LOG_DIR"/node_*.log &
TAIL_PID=$!
echo "$TAIL_PID" >> "$PIDS_FILE"

# Ждём, пока пользователь не нажмёт Ctrl+C
wait