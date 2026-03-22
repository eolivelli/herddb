set -x
./run.sh --password hdb --batch-size 10000 --ingest-threads 12 --dataset gist1m -n 10000001 --checkpoint --drop-table
