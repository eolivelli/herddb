set -x
./run.sh --password hdb --batch-size 20000 --ingest-threads 128 --dataset bigann -n 1000000000 --checkpoint  "$@"
