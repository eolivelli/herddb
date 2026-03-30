set -x
./run.sh --password hdb --batch-size 1000 --ingest-threads 64 -skip-ingest --query-threads 12 --queries 100000 --skip-index --dataset bigann -n 100000000 --checkpoint  "$@"
