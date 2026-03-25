set -x
./run.sh --password hdb --batch-size 20000 --ingest-threads 64 --dataset bigann -n 100000000 --checkpoint --drop-table
