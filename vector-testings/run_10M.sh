set -x
./run.sh --password hdb --batch-size 10000 --ingest-threads 12 --dataset sift10m -n 10000000 --checkpoint
