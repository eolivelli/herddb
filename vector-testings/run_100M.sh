set -x
./run.sh --password hdb --batch-size 1000 --ingest-threads 64 --dataset bigann -n 100000000 --index-before-ingest --checkpoint  "$@"
