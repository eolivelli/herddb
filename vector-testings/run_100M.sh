set -x
./run.sh --password hdb --batch-size 20000 --ingest-threads 8 --dataset bigann -n 100000000 --index-before-ingest --checkpoint  "$@"
