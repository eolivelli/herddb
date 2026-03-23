set -x
./run.sh --password hdb --batch-size 20000 --ingest-threads 24 --dataset sift10m -n 5000000 --checkpoint --drop-table
