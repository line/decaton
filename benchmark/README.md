Benchmark
=========

This module implements performance benchmarking framework for Decaton.

How to run benchmark
====================

Build a shadowJar:
```sh
../gradlew :benchmark:shadowJar
```

Run with arbitrary parameters:
```sh
./debm.sh
 --title "Decaton" \
 --runner com.linecorp.decaton.benchmark.DecatonRunner \
 --tasks 10000 \
 --warmup 100 \
 --simulate-latency=10 \
 --param=decaton.partition.concurrency=20 
```
