# Java Guide

Impress can be used directly from Java. The Java-facing API is centered on
`DynamoStorable` and `DynamoDbMapper`, which remove most of the generic noise
from the lower-level `Storable<AttributeValue, Object>` contract.

## Connect

```java
import it.grational.storage.dynamodb.DynamoDb;

DynamoDb dynamo = new DynamoDb();
```

With an existing AWS SDK v2 client:

```java
import it.grational.storage.dynamodb.DynamoDb;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

DynamoDbClient client = DynamoDbClient.builder()
	.region(Region.EU_WEST_1)
	.build();

DynamoDb dynamo = new DynamoDb(client);
```

## Java POJO

Implement `DynamoStorable` and write the item into the mapper. Return the mapper
so Java callers can chain naturally.

```java
import it.grational.storage.dynamodb.*;
import it.grational.storage.DbMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class User implements DynamoStorable {
	private String id;
	private String email;
	private String name;
	private int loginCount;

	public User() {}

	public User(String id, String email, String name) {
		this.id = id;
		this.email = email;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getLoginCount() {
		return loginCount;
	}

	public void setLoginCount(int loginCount) {
		this.loginCount = loginCount;
	}

	@Override
	public DynamoDbMapper impress(
		DbMapper<AttributeValue, Object> mapper,
		boolean versioned
	) {
		DynamoDbMapper dynamoMapper = (DynamoDbMapper) mapper;

		return dynamoMapper
			.with("id", id)
			.with("email", email)
			.with("name", name)
			.with("loginCount", loginCount);
	}
}
```

Save and load it:

```java
import it.grational.storage.dynamodb.*;

User user = new User("user-1", "ada@example.com", "Ada Lovelace");
dynamo.putItem("users", user);

User loaded = dynamo
	.getItem("users", KeyFilter.of("id", "user-1"), User.class)
	.get();
```

When loading into a JavaBean, Impress uses the no-arg constructor and JavaBean
setters. DynamoDB numbers are converted to compatible primitive and boxed
numeric setter types.

The table schema owns key identity. For normal `DynamoStorable` objects, do not
mark keys in `impress(...)`; Impress can inspect the DynamoDB table schema when
it needs to extract keys for update, delete, and refresh operations.

## Java Record

Records are supported for immutable read models. Implement `DynamoStorable` and
write record components into the mapper.

```java
import it.grational.storage.dynamodb.*;
import it.grational.storage.DbMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public record AuditEvent(
	String streamId,
	long timestamp,
	String eventType
) implements DynamoStorable {
	@Override
	public DynamoDbMapper impress(
		DbMapper<AttributeValue, Object> mapper,
		boolean versioned
	) {
		DynamoDbMapper dynamoMapper = (DynamoDbMapper) mapper;

		return dynamoMapper
			.with("streamId", streamId)
			.with("timestamp", timestamp)
			.with("eventType", eventType);
	}
}
```

```java
AuditEvent event = new AuditEvent(
	"user-1",
	System.currentTimeMillis(),
	"USER_CREATED"
);

dynamo.putItem("events", event);

AuditEvent loaded = dynamo
	.getItem(
		"events",
		KeyFilter.of("streamId", "user-1", "timestamp", event.timestamp()),
		AuditEvent.class
	)
	.get();
```

When loading into a record, Impress calls the canonical record constructor and
converts DynamoDB numeric values to the record component types.

## Dynamic Data With DynamoMap

`DynamoMap` implements `Map<String, Object>`, so it works in Java APIs that
expect a standard map.

```java
import it.grational.storage.dynamodb.DynamoMap;
import java.util.Map;

DynamoMap item = new DynamoMap();
item.put("id", "user-1");
item.put("name", "Ada Lovelace");

Map<String, Object> asMap = item;
dynamo.putItem("users", item);
```

For nested data:

```java
DynamoMap user = new DynamoMap();
user.put("id", "user-1");
user.put("profile", Map.of(
	"department", "Engineering",
	"skills", List.of("Java", "DynamoDB")
));

dynamo.putItem("users", user);
```

## Java-Friendly Mapper Chaining

`DynamoDbMapper` returns `DynamoDbMapper` from its fluent methods, so Java
chains do not collapse back to the generic `DbMapper` type.

```java
DynamoDbMapper mapper = new DynamoMapper()
	.with("id", "user-1")
	.with("name", "Ada")
	.with("score", 42)
	.with("active", true)
	.withNull("nickname")
	.remove("oldField");
```

Nested objects and lists are supported without converting Java collections to
arrays:

```java
List<User> users = List.of(
	new User("u1", "ada@example.com", "Ada"),
	new User("u2", "grace@example.com", "Grace")
);

DynamoDbMapper mapper = new DynamoMapper()
	.with("id", "team-1")
	.withItems("members", users);
```

Use `withMappers` when you already have mapper instances:

```java
List<DynamoMapper> entries = List.of(
	new DynamoMapper().with("id", "a"),
	new DynamoMapper().with("id", "b")
);

DynamoDbMapper mapper = new DynamoMapper()
	.with("id", "container-1")
	.withMappers("entries", entries);
```

## Advanced: Explicit Key Markers

Most application code should not use `FieldType.PARTITION_KEY` or
`FieldType.SORT_KEY`. Prefer plain field mapping and let the table schema define
which fields are keys.

Explicit markers are still useful when you need a standalone `DynamoMapper` to
know its key before it is passed to `DynamoDb`. For example, this works without
consulting DynamoDB:

```java
import static it.grational.storage.dynamodb.FieldType.*;

DynamoDbMapper mapper = new DynamoMapper()
	.with("id", "user-1", PARTITION_KEY)
	.with("createdAt", 1700000000L, SORT_KEY)
	.with("name", "Ada");

Map<String, AttributeValue> key = mapper.key();
```

If the mapper is passed to a `DynamoDb` operation with a table name, explicit
markers are usually unnecessary:

```java
DynamoDbMapper update = new DynamoMapper()
	.with("id", "user-1")
	.with("name", "Ada Lovelace");

dynamo.updateItem("users", update);
```

In the second example, Impress can inspect the `users` table schema and mark the
key fields internally.

## Get, Query, And Scan

Java callers can use `returning(Class<T>)` instead of Groovy’s `as(...)` method.

```java
User user = dynamo
	.getItem("users", KeyFilter.of("id", "user-1"))
	.returning(User.class)
	.fields("id", "email", "name")
	.get();
```

Query:

```java
List<AuditEvent> events = dynamo
	.query("events", KeyFilter.of("streamId", "user-1"))
	.returning(AuditEvent.class)
	.backward()
	.take(20)
	.list();
```

Scan:

```java
import static it.grational.storage.dynamodb.DynamoFilter.*;

List<User> activeUsers = dynamo
	.scan("users")
	.returning(User.class)
	.filter(match("status", "active"))
	.fields("id", "email", "name")
	.take(50)
	.list();
```

## KeyFilter Builder

The static `KeyFilter.of(...)` methods are compact, but the builder reads well
from Java when sort keys or range conditions are involved.

```java
KeyFilter exact = KeyFilter
	.partition("userId", "user-1")
	.sort("createdAt", 1700000000L)
	.build();
```

Range condition:

```java
import static it.grational.storage.dynamodb.DynamoFilter.*;

KeyFilter recent = KeyFilter
	.partition("userId", "user-1")
	.sort(greater("createdAt", 1700000000L))
	.build();

List<DynamoMap> events = dynamo.query("events", recent).list();
```

## Table Creation And Billing

On-demand billing is the default:

```java
dynamo.createTable(
	"users",
	"id",
	new Index[] { Index.of("email") }
);
```

Provisioned billing can be explicit:

```java
dynamo.createTable(
	"users",
	"id",
	new Index[] { Index.of("email") },
	BillingOptions.provisioned(5, 5)
);
```

If table and secondary-index capacity should differ:

```java
dynamo.createTable(
	"orders",
	"customerId",
	"createdAt",
	new Index[] { Index.of("status", "createdAt") },
	BillingOptions.provisioned(
		10, 5,  // table read/write
		3, 2    // index read/write
	)
);
```

## Practical Java Repository

```java
import java.util.List;
import static it.grational.storage.dynamodb.DynamoFilter.*;

public final class UserRepository {
	private final DynamoDb dynamo;
	private final String table;

	public UserRepository(DynamoDb dynamo, String table) {
		this.dynamo = dynamo;
		this.table = table;
	}

	public User findById(String id) {
		return dynamo
			.getItem(table, KeyFilter.of("id", id), User.class)
			.get();
	}

	public List<User> activeUsers(int count) {
		return dynamo
			.scan(table)
			.returning(User.class)
			.filter(match("status", "active"))
			.take(count)
			.list();
	}

	public void save(User user) {
		dynamo.putItem(table, user);
	}
}
```
