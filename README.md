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
- `DynamoMap` for dynamic, map-like records.
- `Dynable` for Groovy domain objects with built-in version support.
- `DynamoStorable` and `DynamoDbMapper` for Java POJOs and records.
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
It includes POJO and record examples, Java-friendly mapper chaining, collection
mapping, and `DynamoMap` as a standard `Map<String, Object>`.

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

The default release line targets Java 17 bytecode and Groovy 5. Compatibility
releases are organized around Groovy major versions because Groovy is also a
runtime dependency:

| Version | Branch | Java | Groovy | Use It When |
| --- | --- | --- | --- | --- |
| `1.1.0` | `main` | 17+ | 5.x | You are on the current Groovy line. |
| `1.1.0-g4` | `groovy-4` | 17+ | 4.x | Your application depends on Groovy 4. |
| `1.1.0-j8g3` | `java-8-groovy-3` | 8+ | 3.x | You are maintaining a Java 8 / Groovy 3 application. |

## License

Impress is open source software released under the MIT License.
