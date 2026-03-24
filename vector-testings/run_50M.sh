set -x
./run.sh --password hdb --batch-size 30000 --ingest-threads 24 --dataset sift10m -n 50000000 --checkpoint --drop-table "@0"
