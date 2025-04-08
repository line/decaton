Decaton
=======

[![Build Status](https://github.com/line/decaton/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/line/decaton/actions?query=workflow%3ACI+branch%3Amaster+event%3Apush)

Decaton is a streaming task processing framework built on top of [Apache Kafka](https://kafka.apache.org/).
It is designed to enable "concurrent processing of records consumed from one partition" which isn't possible in many Kafka consumer frameworks.

Here is the list of property that Decaton enables by default:
* Concurrent (either multi-threaded or asynchronous) processing of records consumed from one partition
* Preserve ordering guarantee by record keys
* Preserve At-least-once delivery semantics no matter in which order does records gets processed

Decaton is a library rather than a full-stack execution environment so you can easily integrate it into your existing/new JVM applications just by adding few lines of artifact dependencies.

Since it has been designed, optimized and being used for [LINE](https://line.me/)'s server system which produces over 1 million, I/O intensive tasks per second for each stream, its internal implementation for enabling concurrent processing of records is highly optimized and can produce ideal throughput with minimal count of servers, maximizing their resource utilization.

# Getting Started / Tutorial

Please see [Getting Started](./docs/getting-started.adoc)

# When to use (and when to not)

It's good idea to use Decaton when you have requirement for high-throughput and/or low-latency processing capability with your processing logic containing I/O access against external systems (e.g, DB access, Web API access) which tends to apply certain processing latency in each tasks.

It would be better idea to look for other frameworks like Kafka Streams when you need to do complex stream processing/aggregation such as streams join, windowed processing without needing to access external storage/web APIs.

# Minimum dependencies

Below is the minimum dependencies to add Decaton artifacts into your Gradle project. It's for people who prefers to try the APIs first by adding it into your project, please see [Getting Started](./docs/getting-started.adoc) for the detailed explanation and proper use of it.

```groovy
// For task producers
implementation "com.linecorp.decaton:decaton-common:$DECATON_VERSION"
implementation "com.linecorp.decaton:decaton-client:$DECATON_VERSION"
// For processors
implementation "com.linecorp.decaton:decaton-common:$DECATON_VERSION"
implementation "com.linecorp.decaton:decaton-processor:$DECATON_VERSION"
```

# Features

The core feature of Decaton is support for concurrent processing of records consumed from one partition. See [Why Decaton](./docs/why-decaton.adoc) part of document for the detail.

Besides that, it got a lot of unique and useful features through its adoption spread for many services at LINE which are all came out of real needs for building services.
Below are some examples. See [Index](./docs/index.adoc) for the full list.
* Retry Queuing - Retry a failed task with back-off without blocking other tasks flow
* Dynamic Rate Limiting - Apply and update processing rate quota dynamically
* Task Compaction - Crush preceding tasks which its processing results will be overwritten by following task

# Performance

High performance is one of the biggest functionalities of Decaton so we're carefully tracking its performance transitions commit-by-commit.
You can see performance tracking dashboard from [here](https://line.github.io/decaton) (Note that the actual performance could be more or less depending on machine resource and configuration).

# How to build

We use Gradle to build this project.

```sh
./gradlew build
```

# How to contribute

See [CONTRIBUTING.md](CONTRIBUTING.md).

If you believe you have discovered a vulnerability or have an issue related to security, please contact the maintainer directly or send us a email to [dl_oss_dev@linecorp.com](mailto:dl_oss_dev@linecorp.com) before sending a pull request.

# License

```
Copyright 2020 LINE Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

See [LICENSE](LICENSE) for more detail.

# Contributor Credit
Below people made great contributions to this project in early days but their name doesn't appear in commit history because we had to erase commit logs when open-sourcing it by security reasons. Thank you so much for your contributions!

[Haruki Okada](https://github.com/ocadaruma), [Wonpill Seo](https://github.com/lovejinstar), [Szuyung Wang](https://github.com/johnny94), [Vincent Pericart](https://github.com/mauhiz), [Ryosuke Hasebe](https://github.com/be-hase), Maytee Chinavanichkit, Junpei Koyama, Yusuke Imai, [Alex Moreno](https://github.com/moleike)
