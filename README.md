<p align="center">
  <img src="resources/logo.png" alt="Impress Logo" width="400"/>
</p>

# Impress

Impress is a small DynamoDB persistence library for Groovy and Java. It wraps
the AWS SDK v2 client with fluent table, item, query, scan, filter, projection,
pagination, and optimistic-locking APIs while keeping the underlying DynamoDB
model visible when you need it.

The library is designed around the impression pattern: your object describes
how it is impressed into a mapper, and Impress handles the DynamoDB request
shape.

## Why Use It

- Natural Groovy and Java APIs over the AWS DynamoDB SDK.
- `DynamoMap` for dynamic, map-like items.
- `Dynable` for Groovy domain objects with built-in version support.
- `DynamoStorable` and `DynamoDbMapper` for Java POJOs.
- Fluent `getItem`, `query`, and `scan` builders.
- Rich filter expressions, including nested paths and escaped literal dots.
- Automatic pagination helpers and explicit paged access.
- Automatic key extraction for update, delete, and refresh workflows.
- Table creation with keys, secondary indexes, and billing options.

## Installation

Impress is published through JitPack.

**Gradle**

```groovy
repositories {
  maven { url = 'https://jitpack.io' }
}

dependencies {
  implementation 'com.github.grational:impress:VERSION'
}
```

**Maven**

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.github.grational</groupId>
  <artifactId>impress</artifactId>
  <version>VERSION</version>
</dependency>
```

## Quick Start

```groovy
import it.grational.storage.dynamodb.*
import static it.grational.storage.dynamodb.DynamoFilter.*

def dynamo = new DynamoDb()

dynamo.createTable(
  'users',
  'id',
  [Index.of('email')] as Index[]
)

dynamo.putItem('users', new DynamoMap(
  id: 'user-1',
  name: 'Ada Lovelace',
  email: 'ada@example.com',
  profile: [
    department: 'Engineering',
    skills: ['Groovy', 'DynamoDB']
  ]
))

DynamoMap user = dynamo.getItem('users', KeyFilter.of('id', 'user-1'))
  .fields('id', 'name', 'profile.department')
  .get()

List<DynamoMap> engineers = dynamo.scan('users')
  .filter(match('profile.department', 'Engineering'))
  .take(25)
  .list()
```

## Documentation

GitHub renders Markdown well, so the documentation is split into focused files:

- [Documentation index](docs/README.md)
- [Groovy guide](docs/groovy.md)
- [Java guide](docs/java.md)
- [Feature guide](docs/features.md)
- [Development and testing](docs/development.md)

Start with the Java guide if you are integrating Impress from a Java service.
It includes Java 8 compatible POJO examples, Java-friendly mapper chaining,
collection mapping, and `DynamoMap` as a standard `Map<String, Object>`.

## Core API

| Type | Purpose |
| --- | --- |
| `DynamoDb` | Main entry point for table, item, query, scan, update, delete, and batch operations. |
| `DynamoMap` | Dynamic map-backed item that can be saved directly. |
| `Dynable` | Groovy base class for versioned domain objects. |
| `DynamoStorable` | Java-friendly DynamoDB storable interface. |
| `DynamoDbMapper` | Java-friendly mapper with covariant fluent returns. |
| `DynamoMapper` | Builds DynamoDB `AttributeValue` maps and update expressions. |
| `KeyFilter` | Partition and sort-key condition builder. |
| `DynamoFilter` | Filter expression builder for scans, queries, and sort-key ranges. |
| `Index`, `Keys`, `Scalar` | Table and secondary-index schema helpers. |
| `BillingOptions` | On-demand or provisioned billing configuration for table creation. |
| `PagedResult` | Items plus continuation metadata for manual pagination. |

## Local Development

The repository includes a DynamoDB Local Docker Compose configuration:

```bash
cd lib
docker compose up -d
```

The local endpoint is exposed at `http://localhost:8888`.

Useful Gradle commands:

```bash
./gradlew :lib:compileTestJava :lib:compileTestGroovy
./gradlew :lib:test --tests it.grational.storage.dynamodb.JavaCompatibilityUSpec
./gradlew :lib:test
```

The full `:lib:test` task includes integration specs that require DynamoDB
Local to be running.

## Compatibility

This branch is configured for Java 8 / Groovy 3 in
[lib/build.gradle](lib/build.gradle). It supports Java POJOs and standard maps;
Java records are not available on this compatibility line.

## License

Impress is open source software released under the MIT License.
