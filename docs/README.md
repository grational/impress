# Impress Documentation

This documentation is organized for GitHub readers:

- Keep the root [README](../README.md) short enough to scan.
- Put language-specific examples in focused guides.
- Keep feature behavior and operational notes in separate pages that can grow
  without making the project front page hard to use.

## Guides

| Guide | Use It When |
| --- | --- |
| [Groovy guide](groovy.md) | You are using Impress from Groovy or Spock tests. |
| [Java guide](java.md) | You are using Impress from a Java service or want Java 8 POJO examples. |
| [Feature guide](features.md) | You need details on querying, filtering, pagination, schema creation, billing, projections, or updates. |
| [Development and testing](development.md) | You are working on Impress itself or running the integration suite. |

## Common Imports

Groovy:

```groovy
import it.grational.storage.dynamodb.*
import static it.grational.storage.dynamodb.DynamoFilter.*
```

Java:

```java
import it.grational.storage.dynamodb.*;
import static it.grational.storage.dynamodb.DynamoFilter.*;
```

## Mental Model

Impress has three main layers:

1. `DynamoDb` executes operations against an AWS SDK v2 `DynamoDbClient`.
2. `DynamoMapper` builds DynamoDB `AttributeValue` maps and update expressions.
3. Your item type implements `Storable` or `DynamoStorable` and describes how
   it should be impressed into a mapper.

For dynamic data, use `DynamoMap`. For Groovy domain objects, use `Dynable`.
For Java domain objects, implement `DynamoStorable`.

## Dependency Version

Examples use `VERSION` as a placeholder. Replace it with a Git tag, release
version, or JitPack-compatible branch/SNAPSHOT version from this repository.

This branch targets Java 8 bytecode and Groovy 3. Use the `-j8g3` suffix for
releases from this branch. The default release line targets Java 17 / Groovy 5,
and the `-g4` suffix targets Java 17 / Groovy 4.
