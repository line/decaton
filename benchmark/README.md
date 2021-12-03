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
./debm.sh \
 --title "Decaton" \
 --runner com.linecorp.decaton.benchmark.DecatonRunner \
 --tasks 10000 \
 --warmup 100 \
 --simulate-latency=10 \
 --param=decaton.partition.concurrency=20 
```

How to run benchmark with custom Runner
=======================================

#### Publish `benchmark` module locally:

```sh
cd /path/to/decaton/benchmark

../gradlew :benchmark:publishToMavenLocal -x sign
```

#### Add `decaton-benchmark` dependency to your custom runner project:

build.gradle

```groovy
repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation "com.linecorp.decaton:decaton-benchmark:$DECATON_VERSION"
}
```

#### Implement com.linecorp.decaton.benchmark.Runner:

CustomRunner.java

```java
package com.example;

import com.linecorp.decaton.benchmark.Recording;
import com.linecorp.decaton.benchmark.ResourceTracker;
import com.linecorp.decaton.benchmark.Runner;
import com.linecorp.decaton.benchmark.Task;

public class CustomRunner implements Runner {
    @Override
    public void init(Config config, Recording recording, ResourceTracker resourceTracker)
            throws InterruptedException {
        // initialize and start your consumer
    }

    @Override
    public void close() throws Exception {
        // cleanup
    }
}
```

#### Build a shadowJar of the custom runner:

Build one using https://github.com/johnrengelman/shadow

### Run benchmark:

```sh
export CLASSPATH=/path/to/custom-runner-all.jar

cd /path/to/decaton/benchmark

./debm.sh \
 --title "Custom" \
 --runner com.example.CustomRunner \
 --tasks 10000 \
 --warmup 100 \
 --simulate-latency=10 
```
