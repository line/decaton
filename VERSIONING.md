Decaton Versioning
==================

Since the release of 1.0.0, this project uses [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html) for versioning its releases.
In short, quoting from the official document's summary section:

Given a version number MAJOR.MINOR.PATCH, increment the:

1. MAJOR version when you make incompatible API changes,
2. MINOR version when you add functionality in a backward-compatible manner, and
3. PATCH version when you make backward-compatible bug fixes.


Public APIs
===========

We consider the following items as Public API and each one of the changes made against those items in a backward-incompatible way are considered a "breaking change", so we bump the MAJOR version for the release shipping the change.


* Public interfaces/classes/enums that are defined by source code in this project and are included in a published JAR, excluding ones with the following conditions.
  * An interface/class/enum that belongs to a package containing subpackage `internal`. e.g, `com.linecorp.decaton.runtime.internal.Aclass`.
  * A constructor/method/field that depends on at least one of internal items in its signature. e.g, `com.linecorp.decaton.LoggingContext`'s constructor that takes `com.linecorp.decaton.runtime.internal.TaskRequest` as an argument.
* Configuration property keys
* Metric names and tags

Anything that is not in the above list IS NOT considered as a Public API.

We DO NOT consider the following items as public APIs (just examples, not a complete list):

* Logging message formats
* Java thread names
* Classes from shadowed and relocated dependency (e.g, `protobuf-java`)
* Classes and resources that are not included in published JAR (e.g, classes related only for testing, benchmarking, etc.)


Backward-compatible
===================

Backward compatible in this document refers to the following definition:

* An application referring to any public APIs can still be compiled without any errors
* An application using any public APIs can still expect similar behavior to the previous version that may differ slightly yet within the level that doesn't affect the application's behavior
* ID-ish strings/values like property keys and metric names kept exactly the same as the previous version


Details of the changes
======================

You can see the list of changes that has been made by particular version at [Releases page](https://github.com/line/decaton/releases).


Guide for developers
====================

It is a developer's (and reviewer's) responsibility to mark each PR with labels indicating three types of changes that the PR introduces.
* PR containing a breaking change should be tagged by label: `breaking change`
* PR containing a new feature should be tagged by label: `new feature`
* PR containing a bugfix should be tagged by label: `bugfix`

It is a release handler's responsibility to properly bump the required part of the version number along with the patch for the intended changes.
Release handler should go through the PRs that will be included in the upcoming release, and decide which part of the version to increment.
Say the current version is `1.2.3`,
* If any of PR introduces a breaking change, the next version is `2.0.0`
* If any of PR introduces a backward-compatible new feature/functionality the next version is `1.3.0`
* If any of PR introduces a backward-compatible bugfixes, the next version is `1.2.4`
