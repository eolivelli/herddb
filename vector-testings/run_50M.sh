set -x
./run.sh --password hdb --batch-size 10000 --ingest-threads 64 --dataset sift10m -n 50000000 --checkpoint --drop-table 
