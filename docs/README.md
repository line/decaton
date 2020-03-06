Decaton Documentation
=====================

Root of documentation is [here](./index.adoc)

# Documentation Guide

* Documents are written [AsciiDoc](http://asciidoc.org/).
* [AsciiDoc cheatsheet](https://powerman.name/doc/asciidoc) can be a good reference.

# Header Format

As an attempt to avoid documentation content get outdated behind the latest of actual code, each documents are required to have following header attributes.

```
:base_version: VERSION // The version of decaton when last the document's content had confirmed to be up-to-date
:modules: MOD1,MOD2.. // List of Gradle modules that the content of this document is relying on
```

Based on those information the test of `docs` module confirms if any documents left behind the latest version of dependent modules and make built to fail for urging update.
