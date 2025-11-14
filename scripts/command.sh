#!/bin/bash

set -e

TYPE=$1
shift  # Remove first argument, rest are additional args

case "$TYPE" in
  local-up)
    find /home/ubuntu/sp1-cluster-wip -name "ELF_ID" -type f -delete
    docker compose -f infra/docker-compose.local.yml up -d
    ;;

  local-down)
    docker compose -f infra/docker-compose.local.yml down
    find /home/ubuntu/sp1-cluster-wip -name "ELF_ID" -type f -delete
    ;;

  manager-up)
    find /home/ubuntu/sp1-cluster-wip -name "ELF_ID" -type f -delete
    docker compose -f infra/docker-compose.manager.yml up -d
    ;;

  manager-down)
    if [ "$1" == "--volumes" ]; then
      docker compose -f infra/docker-compose.manager.yml down --volumes
    else
      docker compose -f infra/docker-compose.manager.yml down
    fi
    find /home/ubuntu/sp1-cluster-wip -name "ELF_ID" -type f -delete
    ;;

  worker-up)
    find /home/ubuntu/sp1-cluster-wip -name "ELF_ID" -type f -delete
    if [ -z "$1" ]; then
      # No range specified, start all workers
      docker compose -f infra/docker-compose.worker.yml up -d
    else
      # Range specified, e.g., gpu{0..6}
      docker compose -f infra/docker-compose.worker.yml up $1 -d
    fi
    ;;

  worker-down)
    docker compose -f infra/docker-compose.worker.yml down
    find /home/ubuntu/sp1-cluster-wip -name "ELF_ID" -type f -delete
    ;;

  worker-logs)
    docker compose -f infra/docker-compose.worker.yml logs "$@"
    ;;

  controller-logs)
    docker compose -f infra/docker-compose.manager.yml logs cpu-node "$@"
    ;;

  request_block)
    if [ -z "$1" ]; then
      echo "Error: Block number required"
      echo "Usage: $0 request_block <block_number>"
      exit 1
    fi
    BLOCK_NUMBER=$1
    ELF_PATH="/home/ubuntu/rsp/bin/client/target/elf-compilation/riscv64im-succinct-zkvm-elf/release/rsp-client"
    INPUT_PATH="/home/ubuntu/rsp/input/input/1/${BLOCK_NUMBER}.bin"

    if [ ! -f "$ELF_PATH" ]; then
      echo "Error: ELF file not found: $ELF_PATH"
      exit 1
    fi

    if [ ! -f "$INPUT_PATH" ]; then
      echo "Error: Input file not found: $INPUT_PATH"
      exit 1
    fi

    cargo run --release --bin sp1-cluster-cli bench input "$ELF_PATH" "$INPUT_PATH"
    ;;

  *)
    echo "Usage: $0 <type> [options]"
    echo ""
    echo "Types:"
    echo "  local-up                      Start local services"
    echo "  local-down                    Stop local services"
    echo "  manager-up                    Start manager services"
    echo "  manager-down [--volumes]      Stop manager services (optionally remove volumes)"
    echo "  worker-up [range]             Start worker services (optionally specify range like gpu{0..6})"
    echo "  worker-down                   Stop worker services"
    echo "  worker-logs [options]         Show worker logs"
    echo "  controller-logs [options]     Show controller (cpu-node) logs"
    echo "  request_block <block_number>  Run benchmark for a specific block number"
    echo ""
    echo "Examples:"
    echo "  $0 local-up"
    echo "  $0 manager-up"
    echo "  $0 manager-down --volumes"
    echo "  $0 worker-up gpu{0..6}"
    echo "  $0 worker-logs -f"
    echo "  $0 controller-logs -f --tail=100"
    echo "  $0 request_block 23772760"
    exit 1
    ;;
esac
