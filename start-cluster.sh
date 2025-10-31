#!/bin/bash

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting SP1 Cluster...${NC}"

# Create logs directory if it doesn't exist
mkdir -p logs

# Start docker compose services
echo -e "${GREEN}[1/4] Starting PostgreSQL and Redis...${NC}"
cd infra && docker compose up -d postgresql redis
cd ..

# Wait a bit for databases to be ready
echo -e "${YELLOW}Waiting for databases to be ready...${NC}"
sleep 3

# Start API server
echo -e "${GREEN}[2/4] Starting API server...${NC}"
cargo build --release --bin sp1-cluster-api
nohup cargo run --release --bin sp1-cluster-api > logs/api.log 2>&1 &
API_PID=$!
echo $API_PID > logs/api.pid
echo -e "API started with PID: $API_PID"

# Wait for API to be ready
sleep 2

# Start coordinator
echo -e "${GREEN}[3/4] Starting coordinator...${NC}"
cargo build --release --bin sp1-cluster-coordinator
nohup cargo run --release --bin sp1-cluster-coordinator > logs/coordinator.log 2>&1 &
COORDINATOR_PID=$!
echo $COORDINATOR_PID > logs/coordinator.pid
echo -e "Coordinator started with PID: $COORDINATOR_PID"

# Wait for coordinator to be ready
sleep 2

# Start worker
echo -e "${GREEN}[4/4] Starting worker...${NC}"
cargo build --release --bin sp1-cluster-node --features gpu
nohup cargo run --release --bin sp1-cluster-node --features gpu > logs/worker.log 2>&1 &
WORKER_PID=$!
echo $WORKER_PID > logs/worker.pid
echo -e "Worker started with PID: $WORKER_PID"

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All services started successfully!${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "API PID: $API_PID"
echo -e "Coordinator PID: $COORDINATOR_PID"
echo -e "Worker PID: $WORKER_PID"
echo -e ""
echo -e "Logs are available in the logs/ directory:"
echo -e "  - logs/api.log"
echo -e "  - logs/coordinator.log"
echo -e "  - logs/worker.log"
echo -e ""
echo -e "To view logs in real-time:"
echo -e "  tail -f logs/api.log"
echo -e "  tail -f logs/coordinator.log"
echo -e "  tail -f logs/worker.log"
echo -e ""
echo -e "To stop all services, run: ${YELLOW}./stop-cluster.sh${NC}"
