#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Stopping SP1 Cluster...${NC}"

# Function to kill process if PID file exists
kill_service() {
    local service_name=$1
    local pid_file=$2

    if [ -f "$pid_file" ]; then
        PID=$(cat "$pid_file")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}Stopping $service_name (PID: $PID)...${NC}"
            kill $PID
            # Wait for process to stop
            for i in {1..10}; do
                if ! ps -p $PID > /dev/null 2>&1; then
                    break
                fi
                sleep 0.5
            done
            # Force kill if still running
            if ps -p $PID > /dev/null 2>&1; then
                echo -e "${RED}Force killing $service_name...${NC}"
                kill -9 $PID
            fi
        else
            echo -e "${RED}$service_name process not running (stale PID file)${NC}"
        fi
        rm "$pid_file"
    else
        echo -e "${RED}No PID file found for $service_name${NC}"
    fi
}

# Stop services in reverse order
kill_service "Worker" "logs/worker.pid"
kill_service "Coordinator" "logs/coordinator.pid"
kill_service "API" "logs/api.pid"

# Stop docker compose services
echo -e "${GREEN}Stopping PostgreSQL and Redis...${NC}"
cd infra && docker compose stop postgresql redis
cd ..

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All services stopped!${NC}"
echo -e "${BLUE}========================================${NC}"
