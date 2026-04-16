# Development And Testing

This guide is for contributors working on Impress.

## Build

```bash
./gradlew :lib:compileGroovy
./gradlew :lib:compileTestJava :lib:compileTestGroovy
```

Java compatibility is intentionally covered by Java test fixtures under
`lib/src/test/java`. Do not rely only on Groovy specs when adding Java-facing
APIs; Java source must compile against the public surface.

## DynamoDB Local

The repository includes Docker Compose configuration in `lib/docker-compose.yml`.

```bash
cd lib
docker compose up -d
```

The local DynamoDB endpoint is:

```text
http://localhost:8888
```

Stop it with:

```bash
cd lib
docker compose down
```

## Tests

Focused Java compatibility and unit-style specs:

```bash
./gradlew :lib:test \
  --tests it.grational.storage.dynamodb.JavaCompatibilityUSpec \
  --tests it.grational.storage.dynamodb.PojoSupportSpec \
  --tests it.grational.storage.dynamodb.IterableOverloadUSpec \
  --tests it.grational.storage.dynamodb.KeyFilterBuilderUSpec
```

Focused billing specs:

```bash
./gradlew :lib:test \
  --tests it.grational.storage.dynamodb.BillingOptionsUSpec \
  --tests it.grational.storage.dynamodb.DynamoDbCreateTableUSpec
```

Full suite:

```bash
./gradlew :lib:test
```

The full suite includes integration specs and expects DynamoDB Local to be
reachable on `localhost:8888`.

## Test Task Note

The Gradle build defines custom `uniTest`, `intTest`, and `funTest` tasks, but
the build script also mutates `gradle.startParameter.excludedTaskNames`. When
verifying changes, prefer explicit `:lib:test --tests ...` invocations unless
you have checked the custom task behavior for the current Gradle configuration.

## Documentation Style

- Keep the root README concise.
- Put durable examples under `docs/`.
- Add Java examples for every public API intended to be pleasant from Java.
- Prefer code that compiles against the current public API over pseudocode.
- When adding a Java-facing API, add or update a Java fixture in
  `lib/src/test/java`.
