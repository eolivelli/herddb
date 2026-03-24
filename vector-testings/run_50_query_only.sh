set -x
./run.sh --password hdb --batch-size 10000 --ingest-threads 12 --dataset sift10m -n 50000000  --queries 1000000 --skip-ingest --skip-index --query-threads 8
