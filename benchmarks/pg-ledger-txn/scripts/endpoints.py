"""Central host endpoint constants shared by runners, census, and campaign validation."""
PG_HOST = '127.0.0.1'
PG_PORT = 15432
PG_CONTAINER_PORT = 5432
PG_URL = f'postgres://postgres:gate2d@{PG_HOST}:{PG_PORT}/ledger'
SCYLLA_HOST = '127.0.0.1'
SCYLLA_PORT = 19042
SCYLLA_NATIVE_PORT = 9042
