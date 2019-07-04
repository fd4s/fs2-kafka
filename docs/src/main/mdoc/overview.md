---
id: overview
title: Overview
---

Functional backpressured streams for consuming and producing Kafka records. Exposes a small interface, while taking care of common functionality: batch consuming and producing records, batched offset commits, offset commit recovery, and topic administration, while also simplifying client configuration.

Documentation is kept up-to-date with new releases, currently documenting v@LATEST_VERSION@ on Scala @DOCS_SCALA_MINOR_VERSION@.

## Getting Started

To get started with [sbt](https://scala-sbt.org), simply add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@MODULE_NAME@" % "@LATEST_VERSION@"
```

Published for Scala @SCALA_PUBLISH_VERSIONS@. For changes, refer to the [release notes](https://github.com/ovotech/fs2-kafka/releases).

For Scala 2.11 and 2.12, enable partial unification by adding the following line to `build.sbt`.

```scala
scalacOptions += "-Ypartial-unification"
```

### Compatibility

Backwards binary-compatibility for the library is guaranteed between patch versions.<br>
For example, `@LATEST_MINOR_VERSION@.x` is backwards binary-compatible with `@LATEST_MINOR_VERSION@.y` for any `x > y`.

Please note binary-compatibility is not guaranteed between milestone releases.

## Dependencies

Has a minimal set of dependencies:

- FS2 v@FS2_VERSION@ ([Documentation](https://fs2.io), [GitHub](https://github.com/functional-streams-for-scala/fs2)), and
- Apache Kafka Client v@KAFKA_VERSION@ ([Documentation](https://kafka.apache.org/@KAFKA_DOCS_VERSION@/documentation.html), [GitHub](https://github.com/apache/kafka)).

## Inspiration

Library is heavily inspired by ideas from [Alpakka Kafka](https://github.com/akka/alpakka-kafka).

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html). Refer to the [license file](https://github.com/ovotech/fs2-kafka/blob/master/license.txt).
