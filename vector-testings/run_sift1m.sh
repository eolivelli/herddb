set -x
./run.sh --password hdb --batch-size 10000 --ingest-threads 12 --dataset sift1m -n 1000000 --checkpoint --drop-table
