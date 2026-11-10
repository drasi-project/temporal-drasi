#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
native="$PWD/native/drasi"
mkdir -p "$native/.verification/build" "$native/.verification/go"
export TMPDIR="$native/.verification/build" GOTMPDIR="$native/.verification/go"
export TEMPORAL_DRASI_PLUGIN_CONFIG="${TEMPORAL_DRASI_PLUGIN_CONFIG:-$native/tests/postgres-catalog.json}"
case "$TEMPORAL_DRASI_PLUGIN_CONFIG" in
    /*) ;;
    *) export TEMPORAL_DRASI_PLUGIN_CONFIG="$PWD/$TEMPORAL_DRASI_PLUGIN_CONFIG" ;;
esac
python3 "$native/tests/inspect-postgres-catalog.py" "$TEMPORAL_DRASI_PLUGIN_CONFIG" \
    "$native/.verification/postgres-public-artifacts.json"
bash "$native/build.sh"
bash "$native/verify-plugins.sh"
container="temporal-drasi-postgres-$$"
created=false
cleanup() {
    if [[ "$created" == true ]]; then
        docker logs "$container" > "$native/.verification/postgres-container.log" 2>&1 || true
        docker rm -f "$container" > /dev/null
    fi
}
trap cleanup EXIT
docker run --name "$container" --label temporal.drasi.verification=postgres \
    --detach --rm --publish 127.0.0.1::5432 \
    --env POSTGRES_DB=watchfixture --env POSTGRES_PASSWORD=verification-only-password \
    postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b \
    -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 \
    -c log_statement=all -c log_replication_commands=on
created=true
docker inspect "$container" --format 'PostgreSQL image: {{.Image}}'
ready=false
for ((attempt=0; attempt<60; attempt++)); do
    if docker exec "$container" pg_isready -h 127.0.0.1 -U postgres -d watchfixture > /dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    echo "The created PostgreSQL fixture did not become ready" >&2
    exit 1
fi
docker exec -i "$container" psql -U postgres -d watchfixture -v ON_ERROR_STOP=1 <<'SQL'
SHOW wal_level;
CREATE TABLE watch_items (id integer PRIMARY KEY, name text NOT NULL, amount integer NOT NULL);
INSERT INTO watch_items VALUES (1, 'initial', 12), (2, 'excluded', 3);
CREATE PUBLICATION drasi_fixture_pub FOR TABLE watch_items;
SELECT * FROM watch_items ORDER BY id;
SQL
export TEMPORAL_DRASI_POSTGRES_CONTAINER="$container"
export TEMPORAL_DRASI_POSTGRES_PORT
TEMPORAL_DRASI_POSTGRES_PORT="$(docker port "$container" 5432/tcp | sed 's/.*://')"
CGO_ENABLED=1 go test -count=1 -tags test_dep,integration,temporal_with_drasi \
    ./service/watch/native -run '^TestNativePostgresQueryFixture$' -v -timeout 5m
docker logs "$container" > "$native/.verification/postgres-container.log" 2>&1
python3 - "$native/.verification/postgres-container.log" <<'PY'
from pathlib import Path
import sys

log = Path(sys.argv[1]).read_text()
slot = log.find("received replication command: CREATE_REPLICATION_SLOT drasi_fixture_slot LOGICAL pgoutput")
snapshot = log.find("statement: START TRANSACTION ISOLATION LEVEL REPEATABLE READ")
if slot < 0 or snapshot < 0 or slot >= snapshot:
    raise SystemExit("PostgreSQL WAL retention was not established before the bootstrap snapshot")
print("PostgreSQL WAL retention preceded the bootstrap snapshot; the fixture did not precreate a slot")
PY
