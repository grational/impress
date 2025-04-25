# Impress

A Groovy library for object persistence with DynamoDB support

## Overview

Impress provides a simple way to store Groovy objects in various storage backends. Currently focused on AWS DynamoDB, it offers a clean mapping system that converts domain objects to storage formats and back.

## Installation

### Gradle

```groovy
repositories {
  maven { url "https://jitpack.io" }
}

dependencies {
  implementation 'com.github.grational:impress:latest.release'
}
```

### Maven

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
  <version>latest.release</version>
</dependency>
```

## Core Concepts

### Storable

The foundation interface for persistable objects:

```groovy
interface Storable<S,B> {
  DbMapper<S,B> impress(DbMapper<S,B> mapper, boolean versioned)
}
```

Where:
- `S`: Storage-specific type (e.g., AttributeValue for DynamoDB)
- `B`: Builder type for reconstruction

### DbMapper

Defines how to map object fields to storage formats:

```groovy
interface DbMapper<S,B> {
  DbMapper<S,B> with(String k, String s)
  DbMapper<S,B> with(String k, Number n)
  DbMapper<S,B> with(String k, boolean b)
  DbMapper<S,B> with(String k, DbMapper<S,B> dm, boolean version)
  DbMapper<S,B> with(String k, String... ls)
  DbMapper<S,B> with(String k, Number... ln)
  DbMapper<S,B> with(String k, boolean v, Storable<S,B>... ast)
  DbMapper<S,B> with(String k, boolean v, DbMapper<S,B>... adm)
  Map<String,S> storer(boolean version)
  Map<String,B> builder(boolean version)
}
```

## DynamoDB Implementation

### Dynable

Base class for objects stored in DynamoDB:

```groovy
abstract class Dynable implements Storable<AttributeValue,Object> {
  protected Integer v = 0  // For versioning

  protected abstract DbMapper<AttributeValue,Object> inpress(DynamoMapper mapper)
  abstract DynamoKey key()
}
```

### DynamoDb

Main API for DynamoDB operations:

```groovy
// Create client
DynamoDb dynamo = new DynamoDb()

// Save item
dynamo.putItem("tableName", item)

// Get item by key
Item item = dynamo.objectByKey("tableName", key, Item.class)

// Query by index
List<Item> items = dynamo.objectsQuery("tableName", "indexName", key, Item.class)

// Scan entire table
List<Item> allItems = dynamo.scan("tableName", Item.class)

// Delete item
dynamo.deleteItem("tableName", key)
```

### DynamoKey

Create keys for DynamoDB operations:

```groovy
// Partition key only
DynamoKey key = new DynamoKey("id", "abc123")

// Partition and sort key
DynamoKey compositeKey = new DynamoKey("userId", "user1", "timestamp", 1234567890)
```

### DynamoFilter

Build query and scan filters with a fluent API:

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

// Basic filters
def activeFilter = match("status", "active")
def highPriorityFilter = greater("priority", 7)

// String operations
def nameFilter = beginsWith("name", "J")
def descFilter = contains("description", "important")

// Attribute comparisons
def priceFilter = attributeGreaterThan("price", "cost")

// Combine filters - automatically optimizes expressions without redundant parentheses
def complexFilter = activeFilter.and(highPriorityFilter.or(nameFilter))

// IN operator - uses native DynamoDB IN operator for efficiency
def categoryFilter = in("category", "books", "electronics", "clothing")
def priceRangeFilter = in("price", 10, 20, 30)

// BETWEEN operator (range values)
def dateRangeFilter = between("date", "2023-01-01", "2023-12-31")
def valueRangeFilter = between("value", 100, 500)

// Use with queries
List<Item> items = dynamo.objectsQuery (
  "tableName",
  "indexName",
  key,
  Item.class,
  filter
)
```

## Quick Example

```groovy
import it.grational.storage.dynamodb.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static it.grational.storage.dynamodb.FieldType.*

// 1. Define your domain class
class User extends Dynable {
  String id
  String username
  String email

  User() {}

  User(Map<String, Object> builder) {
    this.id = builder.id
    this.username = builder.username
    this.email = builder.email
  }

  @Override
  protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
    return mapper
      .with('id', id, PARTITION_KEY)
      .with('username', username)
      .with('email', email)
  }

  @Override
  DynamoKey key() {
    return new DynamoKey('id', id)
  }
}

// 2. Create DynamoDB client
def dynamoDb = new DynamoDb()

// 3. Create table
dynamoDb.createTable("users", "id", null, ["email-index": "email"])

// 4. Create and save user
def user = new User(id: "user1", username: "john", email: "john@example.com")
dynamoDb.putItem("users", user)

// 5. Retrieve by key
User retrievedUser = dynamoDb.objectByKey("users", new DynamoKey("id", "user1"), User)

// 6. Query by email index with filter
def activeFilter = match("username", "john")
List<User> users = dynamoDb.objectsQuery (
  "users",
  "email-index",
  new DynamoKey("email", "example.com"),
  User,
  activeFilter
)
```

## Advanced Features

### Versioning

Automatic optimistic locking:

```groovy
// Enable versioning (default, no need to add the extra true param)
dynamoDb.putItem("users", user, true)

// Disable versioning
dynamoDb.putItem("users", user, false)
```

### String Comparison

Compare attribute values against other attributes:

```groovy
// Check if price is greater than cost
def filter = attributeGreaterThan("price", "cost")

// Check if endDate is after startDate
def dateFilter = attributeGreaterThan("endDate", "startDate")
```

### IN Operator

Check if an attribute's value matches any from a provided list. Uses DynamoDB's native IN operator for optimized performance:

```groovy
// Check if category is one of multiple values
def categoryFilter = in("category", "books", "electronics", "clothing")

// Check if price is one of several values
def priceFilter = in("price", 10, 20, 30)
```

### BETWEEN Operator

Check if an attribute's value falls within a range (inclusive):

```groovy
// Check if date is within a range
def dateRangeFilter = between("date", "2023-01-01", "2023-12-31")

// Check if value is within a numeric range
def valueRangeFilter = between("value", 100, 500)
```

### Collection Support

Store collections of values using varargs:

```groovy
// Strings collection
mapper.with("tags", "important", "urgent", "follow-up")

// Numbers collection
mapper.with("validYears", 2022, 2023, 2024)

// Storable objects collection
mapper.with("addresses", true, address1, address2, address3)

// DbMapper objects collection
mapper.with("configurations", true, config1, config2, config3)
```

### Query by Partition Key Only

Query tables using just the partition key:

```groovy
// Create partition-only key
DynamoKey partitionKey = key.partition()

// Query with partition key only
List<Item> items = dynamoDb.objectsQuery("tableName", partitionKey, Item.class)
```

### Batch Operations

Store multiple items at once:

```groovy
def users = [
  new User(id: "user1", username: "john", email: "john@example.com"),
  new User(id: "user2", username: "jane", email: "jane@example.com")
]

dynamoDb.putItems("users", users)
```

### Local Development

For testing with DynamoDB Local:

```groovy
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import java.net.URI

def localClient = DynamoDbClient.builder()
  .endpointOverride('http://localhost:8000'.toURI())
  .build()

def dynamoDb = new DynamoDb(localClient)
```

## Compatibility

- main branch targest Java 21 and Groovy 4.x for maximum performance
- a version with support to Java 8 and Groovy 3.0.24 or later is available with postfix `$version-j8g3`
- AWS DynamoDB SDK 2.31.22 or later

## Contributing

Contributions are welcome! See the repository for more details.

<!-- vim: ts=2:sts=2:sw=2:et=false -->
