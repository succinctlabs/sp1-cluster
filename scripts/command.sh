#!/bin/bash

set -e

TYPE=$1
shift  # Remove first argument, rest are additional args

case "$TYPE" in
  local-up)
    docker compose -f infra/docker-compose.local.yml up -d
    ;;

  local-down)
    docker compose -f infra/docker-compose.local.yml down
    ;;

  manager-up)
    docker compose -f infra/docker-compose.manager.yml up -d
    ;;

  manager-down)
    if [ "$1" == "--volumes" ]; then
      docker compose -f infra/docker-compose.manager.yml down --volumes
    else
      docker compose -f infra/docker-compose.manager.yml down
    fi
    ;;

  worker-up)
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
    ;;

  worker-logs)
    docker compose -f infra/docker-compose.worker.yml logs "$@"
    ;;

  controller-logs)
    docker compose -f infra/docker-compose.manager.yml logs cpu-node "$@"
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
    echo ""
    echo "Examples:"
    echo "  $0 local-up"
    echo "  $0 manager-up"
    echo "  $0 manager-down --volumes"
    echo "  $0 worker-up gpu{0..6}"
    echo "  $0 worker-logs -f"
    echo "  $0 controller-logs -f --tail=100"
    exit 1
    ;;
esac
