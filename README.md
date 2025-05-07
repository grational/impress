<p align="center">
  <img src="resources/logo.png" alt="Impress Logo" width="400"/>
</p>

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

// Get item by key (specifying target class)
Item item = dynamo.objectByKey("tableName", key, Item.class)

// Get item by key (using DynamoMap as default)
DynamoMap map = dynamo.objectByKey("tableName", key)

// Query by index (specifying target class)
List<Item> items = dynamo.objectsQuery("tableName", "indexName", key, filter, Item.class)

// Query with paged results (new feature!)
PagedResult<Item> paged = dynamo.objectsQuery("tableName", "indexName", key, filter, Item.class, limit)
paged.items    // List of items
paged.more     // Are there more results?
paged.last     // Last evaluated key for next page
paged.count    // Number of items in current page

// Query with ordering control (new feature!)
List<Item> ascending = dynamo.objectsQuery("tableName", key, filter, Item.class, true)   // Forward order
List<Item> descending = dynamo.objectsQuery("tableName", key, filter, Item.class, false) // Backward order

// Scan entire table (specifying target class)
List<Item> allItems = dynamo.scan("tableName", filter, Item.class)

// Scan entire table (using DynamoMap as default)
List<DynamoMap> allMaps = dynamo.scan("tableName")

// Delete single item
dynamo.deleteItem("tableName", key)

// Delete multiple items by key and optional filter
int count = dynamo.deleteItems("tableName", key, filter)

// Delete multiple items using an index
int count = dynamo.deleteItems("tableName", "indexName", key, filter)

// Delete multiple items using a scan (full table scan with optional filter)
int count = dynamo.deleteItemsScan("tableName", filter)
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
// For all filters and static operators (every, any)
import static it.grational.storage.dynamodb.DynamoFilter.*

// Basic filters
def activeFilter = match("status", "active")
def highPriorityFilter = greater("priority", 7)

// String operations
def nameFilter = beginsWith("name", "J")
def descFilter = contains("description", "important")

// Attribute comparisons
def priceFilter = attributeGreaterThan("price", "cost")

// Combine filters - automatically optimizes expressions (DynamoDB parser doesn't allow redundant brackets)
def complexFilter = activeFilter.and(highPriorityFilter.or(nameFilter))

// Static logical operators for better readability
def complexFiltersStatic = every(activeFilter, any(highPriorityFilter, nameFilter))

// MATCH-ANY operator - uses native DynamoDB IN operator for efficiency
def categoryFilter = matchAny("category", "books", "electronics", "clothing")
def priceRangeFilter = matchAny("price", 10, 20, 30)

// BETWEEN operator (range values)
def dateRangeFilter = between("date", "2023-01-01", "2023-12-31")
def valueRangeFilter = between("value", 100, 500)

// Use with queries (specifying target class)
List<Item> items = dynamo.objectsQuery (
  "tableName", 
  "indexName",
  key,
  filter,
  Item.class
)

// Use with queries (using DynamoMap as default)
List<DynamoMap> maps = dynamo.objectsQuery (
  "tableName",
  "indexName",
  key,
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

// 5. Retrieve by key (specific class)
User retrievedUser = dynamoDb.objectByKey("users", new DynamoKey("id", "user1"), User)

// 5a. Retrieve by key (using DynamoMap)
DynamoMap userMap = dynamoDb.objectByKey("users", new DynamoKey("id", "user1"))
// Direct access to fields via @Delegate
String username = userMap.username
String email = userMap.email

// 6. Query by email index with filter (specific class)
def activeFilter = match("username", "john")
List<User> users = dynamoDb.objectsQuery (
  "users",
  "email-index",
  new DynamoKey("email", "example.com"),
  activeFilter,
  User
)

// 6a. Query by email index with filter (using DynamoMap)
List<DynamoMap> userMaps = dynamoDb.objectsQuery (
  "users",
  "email-index",
  new DynamoKey("email", "example.com"),
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

### MATCH-ANY Operator

Check if an attribute's value matches any from a provided list. Uses DynamoDB's native IN operator for optimized performance:

```groovy
// Check if category is one of multiple values
def categoryFilter = matchAny("category", "books", "electronics", "clothing")

// Check if price is one of several values
def priceFilter = matchAny("price", 10, 20, 30)
```

### BETWEEN Operator

Check if an attribute's value falls within a range (inclusive):

```groovy
// Check if date is within a range
def dateRangeFilter = between("date", "2023-01-01", "2023-12-31")

// Check if value is within a numeric range
def valueRangeFilter = between("value", 100, 500)
```

### DynamoMap Direct Field Access

DynamoMap now provides direct access to its internal data map through the use of Groovy's `@Delegate` annotation, making it more convenient to work with retrieved data:

```groovy
// Get item using default DynamoMap target class
DynamoMap user = dynamoDb.objectByKey("users", new DynamoKey("id", "user1"))

// Direct field access without using the 'data' property
String username = user.username
String email = user.email
Long timestamp = user.timestamp

// Map-like operations are also available
user.each { key, value -> 
    println "$key: $value" 
}

// Check if a field exists
if (user.containsKey('address')) {
    // Process address
}

// Get all keys
Set<String> fields = user.keySet()
```

### Static Logical Operators

For more readable filter combinations, you can use the static logical operators:

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

// Using instance methods
def filter1 = activeFilter.and(highPriorityFilter.or(nameFilter))

// Using static methods (same result, more readable)
def filter2 = every(activeFilter, any(highPriorityFilter, nameFilter))

// Multiple conditions
def complexFilter = every (
  isNotBlank("name"),
  match("status", "active"),
  any (
    greater("priority", 5),
    contains("tags", "urgent")
  )
)
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

Delete multiple items in a batch:

```groovy
// Delete items by partition key
int deleted = dynamoDb.deleteItems (
  "users",
  new DynamoKey("status", "inactive")
)

// Delete items by partition key with additional filter
int deleted = dynamoDb.deleteItems (
  "users",
  new DynamoKey("status", "inactive"), 
  match("lastLogin", "2022-01-01")
)

// Delete items using an index
int deleted = dynamoDb.deleteItems(
  "users",
  "email-index", 
  new DynamoKey("domain", "example.com"), 
  match("active", false)
)

// Delete items by scanning the entire table with a filter
int deleted = dynamoDb.deleteItemsScan (
  "users", 
   every (
    match("status", "deleted"),
    less("lastAccess", "2023-01-01")
   )
)
```

### Paginated Queries

Query with pagination support using `PagedResult`:

```groovy
// Query with limit for pagination
PagedResult<User> page1 = dynamoDb.objectsQuery (
  "users",
  new DynamoKey("id", "user1"),
  null,    // no filters in this example
  User.class,
  5     // Limit to 5 items per page
)

// Access results
page1.items    // List of User objects
page1.more     // true if more results available
page1.last     // Last evaluated key for next page
page1.count    // Number of items in this page

// Get next page if more results exist
if (page1.more) {
  PagedResult<User> page2 = dynamoDb.objectsQuery (
    "users",
    new DynamoKey("id", "user1"),
    null,    // no filters in this example
    User.class,
    5,          // Limit
    page1.last  // Last evaluated key from previous page
  )
}
```

### Query Ordering

Control the ordering of query results using the forward parameter:

```groovy
// Query with ascending order (default)
List<Item> ascending = dynamoDb.objectsQuery (
  "users",
  new DynamoKey("id", "user1"),
  null,    // no filters in this example
  Item.class,
  true     // Forward order (oldest to newest if sort key is timestamp)
)

// Query with descending order
List<Item> descending = dynamoDb.objectsQuery (
  "users",
  new DynamoKey("id", "user1"),
  null,    // no filters in this example
  Item.class,
  false    // Backward order (newest to oldest if sort key is timestamp)
)

// Complete query method with all parameters
PagedResult<Item> result = dynamoDb.objectsQuery (
  "users",               // Table name
  "email-index",        // Index name (optional)
  new DynamoKey("email", "test@example.com"), // Key condition
  match("active", true), // Filter expression (optional)
  Item.class,           // Target class
  10,                   // Limit (for pagination)
  null,                 // Last evaluated key from previous query
  false                 // Backward order (newest first)
)
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

- Main branch targets Java 21 and Groovy 4.x for maximum performance
- Java 8 and Groovy 3.x compatible version is available on the `java-8-groovy-3` branch released as `$version-j8g3`
- AWS DynamoDB SDK 2.31.22 or later

## Contributing

Contributions are welcome! See the repository for more details.
