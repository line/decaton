processor module
================

A Decaton module containing core classes for processor.


# Benchmark

To maintain good performance for core implementations that impacts processor performance significantly, some micro benchmarks are maintained.
See [jmh-gradle-plugin](https://github.com/melix/jmh-gradle-plugin) for added build targets.

To run benchmark, it's highly recommended running without Gradle daemon to get consistent result (saw it runs with old impl/settings several times).

```sh
./gradlew --no-daemon clean processor:jmh
```
