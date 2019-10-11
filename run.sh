#!/bin/sh
nohup /usr/bin/racket -l errortrace -t srv.rkt -- \
  --key "$1" --username "$2" --no-browser --port 2222 \
  > root/logs/run.log 2>&1 &
