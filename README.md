<p align="center">
  <img src="resources/logo.png" alt="Impress Logo" width="400"/>
</p>

A Groovy library leveraging the impression pattern for object persistence with a concrete DynamoDB implementation.

## Overview

Impress provides a way to store objects in various storage backends using the impression pattern. The current implementation is focused on the NoSQL AWS DynamoDB, offering a clean mapping system that converts domain objects to storage formats and back.

## Recent Changes

### Field Projection Support
The `getItem()`, `query()`, and `scan()` methods now support field projection to retrieve only specific fields from DynamoDB:

- **Reduced Data Transfer**: Specify exactly which fields to retrieve using projection parameters
- **Cost Optimization**: Lower read capacity consumption when projecting smaller field sets
- **Performance Improvement**: Faster response times with smaller payloads
- **Security**: Exclude sensitive fields from query results

The projection feature maintains full backward compatibility - existing code continues to work without changes.

### Pagination Enhancement
The `query()` methods now offer two distinct approaches for handling pagination, distinguished by their return type:

- **Automatic Pagination**: `query()` methods returning `List<T>` automatically retrieve **all** results by handling pagination internally. You no longer need to manually manage `lastEvaluatedKey` for most use cases.
- **Manual Pagination Control**: `query()` methods returning `PagedResult<T>` provide fine-grained control over pagination when you specify a `limit` parameter. These are perfect for managing large datasets or implementing UI pagination.

This elegant approach maintains backward compatibility while providing the flexibility to choose the right pagination strategy simply based on how you call the method.

## Stability
Every version of the library has been thouroughly tested but the API interface could be subject to changes due to the active devlopment following the integration of missing features.

## Installation

### Gradle

```groovy
repositories {
  maven { url 'https://jitpack.io' }
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
  abstract KeyFilter key()
}
```

### DynamoDb

Main API for DynamoDB operations:

```groovy
// Create client
DynamoDb dynamo = new DynamoDb()

// Save item
dynamo.putItem('tableName', item)

KeyFilter keyMatch = KeyFilter.of('key', 'value')

// Get item by key (specifying target class)
Item item = dynamo.getItem (
  'tableName',
  keyMatch,
  Item.class
)

// Get item by key (using DynamoMap as default)
DynamoMap map = dynamo.getItem('tableName', keyMatch)

// Get item with field projection (only specific fields)
DynamoMap partialMap = dynamo.getItem (
  'tableName',
  keyMatch,
  ['id', 'name', 'email']  // Only retrieve these fields
)

// Query by index - automatic pagination (gets ALL results)
List<DynamoMap> items = dynamo.query (
  'tableName',
  'indexName',
  keyMatch
)

// Query by index - manual pagination control (requires limit parameter)
PagedResult<DynamoMap> pagedItems = dynamo.query (
  'tableName',
  'indexName',
  keyMatch,
  null,      // no filter
  DynamoMap.class,
  10         // limit to 10 items per page
)

// Query with sort key range conditions
KeyFilter rangeKeyMatch = KeyFilter.of (
  'userId', 'user123',
  greater('timestamp', 1642681200)
)
List<DynamoMap> recentItems = dynamo.query (
  'tableName',
  rangeKeyMatch
)

DynamoFilter filters = match('otherField', 'value')

// Query by index (with filters)
List<DynamoMap> items = dynamo.query (
  'tableName',
  'indexName',
  keyMatch,
  filters
)

// Query by index (with a target class different by DynamoMap)
List<Item> items = dynamo.query (
  'tableName',
  'indexName',
  keyMatch,
  Item.class
)

// Query by index (specifying filters and target class)
List<Item> items = dynamo.query (
  'tableName',
  'indexName',
  keyMatch,
  filters,
  Item.class
)

// Query with automatic pagination - retrieves ALL results
List<Item> allItems = dynamo.query (
  'tableName',
  'indexName',
  keyMatch,
  filters,
  Item.class
)

// Query with manual pagination control (specify limit for PagedResult)
PagedResult<Item> paged = dynamo.query (
  'tableName',
  'indexName',
  keyMatch,
  filters,
  Item.class,
  limit
)
paged.items    // List of items in this page
paged.more     // Are there more results?
paged.last     // Last evaluated key for next page
paged.count    // Number of items in current page

// Query with ordering control (new feature!)
List<Item> ascending = dynamo.query (
  'tableName',
  key,
  filters,
  Item.class,
  true // forward order
)
List<Item> descending = dynamo.query (
  'tableName',
  key,
  filters,
  Item.class,
  false // Backward order
)

// Scan entire table (no filters nor target class)
List<DynamoMap> allItems = dynamo.scan('tableName')

// with filters
List<DynamoMap> allItems = dynamo.scan('tableName', filters)

// with filters and target class
List<Item> allMaps = dynamo.scan('tableName', filters, Item.class)

// Scan with projection - retrieve only specific fields
List<DynamoMap> projectedItems = dynamo.scan (
  'tableName',
  ['id', 'name', 'status']  // Only retrieve these fields
)

// Scan with filter and projection combined
List<DynamoMap> filteredProjected = dynamo.scan (
  'tableName',
  match('active', true),      // Filter condition
  ['id', 'name', 'email'],    // Projected fields
  DynamoMap                   // Target class
)

// Full-featured scan with projection and advanced options
List<DynamoMap> advancedScan = dynamo.scan (
  'tableName',
  match('department', 'engineering'),  // Filter
  ['id', 'name', 'role'],              // Projection
  DynamoMap,                           // Target class  
  50                                   // Evaluation limit
)

// Delete single item
dynamo.deleteItem('tableName', key)

// Delete multiple items by key and optional filter
int count = dynamo.deleteItems('tableName', key, filter)

// Delete multiple items using an index
int count = dynamo.deleteItems('tableName', 'indexName', key, filter)

// Delete multiple items using a scan (full table scan with optional filter)
int count = dynamo.deleteItems('tableName', filter)

// Remove specific attributes from an item
dynamo.removeAttributes('tableName', keyMatch, 'attribute1', 'attribute2')
```

### Keys and KeyFilter

These classes work together to define key structures for DynamoDB operations:

#### Keys

Represents DynamoDB key schema definition for tables and indexes:

```groovy
// Define table or index keys (string scalar values only)
Keys tableKeys = Keys.of('userId', 'timestamp')

// define keys with different scalar types
Keys tableKeys = Keys.of (
  Scalar.of('userId', ScalarAttributeType.N),
  Scalar.of('binaryData', ScalarAttributeType.B)
)

// Access key components
Scalar partitionKey = tableKeys.partition
Optional<Scalar> sortKey = tableKeys.sort

// Get all attributes as a list
List<Scalar> keyAttributes = tableKeys.attributes()
```

#### KeyFilter

Create key conditions for DynamoDB operations with comprehensive type support:

```groovy
// Partition key only (various data types)
KeyFilter stringKey = KeyFilter.of('id', 'abc123')
KeyFilter numericKey = KeyFilter.of('id', 12345)
KeyFilter binaryKey = KeyFilter.of('id', byteArray)

// Composite keys (partition + sort)
KeyFilter compositeKey = KeyFilter.of('userId', 'user1', 'timestamp', 1234567890)
KeyFilter mixedTypes = KeyFilter.of('userId', 'user1', 'score', 95.5)

// Complex key conditions with range filters
import static it.grational.storage.dynamodb.DynamoFilter.*

// Partition key with sort key range condition
KeyFilter rangeKey = KeyFilter.of (
  'userId', 'user1',
  greater('timestamp', 1642681200)  // Sort key > timestamp
)

// Partition key with sort key BETWEEN condition
KeyFilter betweenKey = KeyFilter.of (
  'userId', 'user1', 
  between('timestamp', 1642681200, 1642767600)
)

// Access individual components
KeyFilter partitionOnly = compositeKey.partition()
Optional<KeyFilter> sortOnly = compositeKey.sort()

// Convert to DynamoDB map format
Map<String, AttributeValue> keyMap = compositeKey.toMap()

// Build key condition expressions for queries
String condition = rangeKey.condition()
Map<String, String> names = rangeKey.conditionNames()
Map<String, AttributeValue> values = rangeKey.conditionValues()
```

### DynamoFilter

Build query and scan filters with a fluent API:

```groovy
// For all filters and static operators (every, any)
import static it.grational.storage.dynamodb.DynamoFilter.*

// Basic filters
def activeFilter = match('status', 'active')
def highPriorityFilter = greater('priority', 7)

// String operations
def nameFilter = beginsWith('name', 'J')
def descFilter = contains('description', 'important')

// Attribute comparisons
def priceFilter = attributeGreaterThan('price', 'cost')

// Nested field filters - use dot notation for path
def premiumUserFilter = match('user.subscription.tier', 'premium')
def highScoreFilter = greater('user.stats.score', 90)
def addressFilter = contains('user.address.city', 'New York')

// Combine filters - automatically optimizes expressions (DynamoDB parser doesn't allow redundant brackets)
def complexFilter = activeFilter.and(highPriorityFilter.or(nameFilter))

// Static logical operators for better readability
def complexFiltersStatic = every(activeFilter, any(highPriorityFilter, nameFilter))

// MATCH-ANY operator - uses native DynamoDB IN operator for efficiency
def categoryFilter = matchAny('category', 'books', 'electronics', 'clothing')
def priceRangeFilter = matchAny('price', 10, 20, 30)

// BETWEEN operator (range values)
def dateRangeFilter = between('date', '2023-01-01', '2023-12-31')
def valueRangeFilter = between('value', 100, 500)

// Use with queries (specifying target class)
List<Item> items = dynamo.query (
  'tableName', 
  'indexName',
  key,
  filter,
  Item.class
)

// Use with queries (using DynamoMap as default)
List<DynamoMap> maps = dynamo.query (
  'tableName',
  'indexName',
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
  KeyFilter key() {
    return KeyFilter.of('id', id)
  }
}

// 2. Create DynamoDB client
def dynamoDb = new DynamoDb()

// 3. Create table - retrocompatibility method
dynamoDb.createTable (
  'users',
  'id',
  ['email-index': 'email'] // simple index with string only field
)

// 3a. Create table - Using composite keys and Index classes
dynamoDb.createTable (
  'users',
  'id',
  'sortKey',
  Index.of('email'),    // 'email-index' automatic name
  Index.of('username')  // Auto-generated index name: username-index
)

// 4. Create and save user
def user = new User(id: 'user1', username: 'john', email: 'john@example.com')
dynamoDb.putItem('users', user)

// 4a. Create and save a user with nested fields using DynamoMap
def userWithProfile = new DynamoMap (
  id: 'user2',
  username: 'jane',
  email: 'jane@example.com',
  profile: [
    active: true,
    preferences: [
      theme: 'dark',
      language: 'en-US'
    ],
    address: [
      street: '123 Main St',
      city: 'New York',
      zipcode: '10001'
    ]
  ]
)
dynamoDb.putItem('users', userWithProfile)

// 5. Retrieve by key (specific class)
User retrievedUser = dynamoDb.getItem('users', KeyFilter.of('id', 'user1'), User)

// 5a. Retrieve by key (using DynamoMap)
DynamoMap userMap = dynamoDb.getItem('users', KeyFilter.of('id', 'user1'))
// Direct access to fields via @Delegate
String username = userMap.username
String email = userMap.email

// 6. Query by email index with filter (specific class)
def activeFilter = match('username', 'john')
List<User> users = dynamoDb.query (
  'users',
  'email-index',
  KeyFilter.of('email', 'example.com'),
  activeFilter,
  User
)

// 6a. Query by email index with filter (using DynamoMap)
List<DynamoMap> userMaps = dynamoDb.query (
  'users',
  'email-index',
  KeyFilter.of('email', 'example.com'),
  activeFilter
)

// 7. Query using nested field filter
def darkThemeFilter = match('profile.preferences.theme', 'dark')
List<DynamoMap> darkThemeUsers = dynamoDb.scan(
  'users',
  darkThemeFilter
)
```

## Advanced Features

### Versioning

Automatic optimistic locking:

```groovy
// Enable versioning (default, no need to add the extra true param)
dynamoDb.putItem('users', user, true)

// Disable versioning
dynamoDb.putItem('users', user, false)
```

### String Comparison

Compare attribute values against other attributes:

```groovy
// Check if price is greater than cost
def filter = attributeGreaterThan('price', 'cost')

// Check if endDate is after startDate
def dateFilter = attributeGreaterThan('endDate', 'startDate')
```

### MATCH-ANY Operator

Check if an attribute's value matches any from a provided list. Uses DynamoDB's native IN operator for optimized performance:

```groovy
// Check if category is one of multiple values
def categoryFilter = matchAny('category', 'books', 'electronics', 'clothing')

// Check if price is one of several values
def priceFilter = matchAny('price', 10, 20, 30)
```

### BETWEEN Operator

Check if an attribute's value falls within a range (inclusive):

```groovy
// Check if date is within a range
def dateRangeFilter = between('date', '2023-01-01', '2023-12-31')

// Check if value is within a numeric range
def valueRangeFilter = between('value', 100, 500)
```

### DynamoMap Direct Field Access

DynamoMap now provides direct access to its internal data map through the use of Groovy's `@Delegate` annotation, making it more convenient to work with retrieved data:

```groovy
// Get item using default DynamoMap target class
DynamoMap user = dynamoDb.getItem('users', KeyFilter.of('id', 'user1'))

// Direct field access without using the 'data' property
String username = user.username
String email = user.email
Long timestamp = user.timestamp

// Map-like operations are also available
user.each { key, value -> 
    println '$key: $value' 
}

// Check if a field exists
if (user.containsKey('address')) {
    // Process address
}

// Get all keys
Set<String> fields = user.keySet()
```

### Nested Fields Support

Access and filter by nested object properties using dot notation:

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

// Store nested object structures
def user = new DynamoMap (
  id: 'user123',
  profile: [
    name: 'John Doe',
    settings: [
      theme: 'dark',
      notifications: true
    ],
    address: [
      street: '123 Main St',
      city: 'San Francisco',
      zipcode: '94105'
    ]
  ]
)
dynamoDb.putItem('users', user)

// Query with filters on nested fields
def darkThemeUsers = dynamoDb.scan (
  'users',
  match('profile.settings.theme', 'dark')
)

// Combine multiple nested field conditions
def activeCaliforniaUsers = dynamoDb.scan (
  'users',
  every (
    match('profile.settings.notifications', true),
    contains('profile.address.city', 'Francisco')
  )
)

// Compare nested numeric fields
def highScoreUsers = dynamoDb.scan (
  'users',
  greater('profile.stats.score', 90)
)
```

The nested path notation works with all filter operations including:
- Equality checks: `match('user.status', 'active')`
- Comparisons: `greater('user.stats.score', 80)`
- String operations: `contains('user.bio', 'developer')`
- Existence checks: `isNotBlank('user.profile.image')`

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
  isNotBlank('name'),
  match('status', 'active'),
  any (
    greater('priority', 5),
    contains('tags', 'urgent')
  )
)
```

### Collection Support

Store collections of values using varargs:

```groovy
// Strings collection
mapper.with('tags', 'important', 'urgent', 'follow-up')

// Numbers collection
mapper.with('validYears', 2022, 2023, 2024)

// Storable objects collection
mapper.with('addresses', true, address1, address2, address3)

// DbMapper objects collection
mapper.with('configurations', true, config1, config2, config3)
```

### Query by Partition Key Only

Query tables using just the partition key:

```groovy
// Create partition-only key
KeyFilter partitionKey = key.partition()

// Query with partition key only
List<Item> items = dynamoDb.query('tableName', partitionKey, Item.class)
```

### Index Definition

Create secondary indexes with more control:

```groovy
// Simple index creation with string parameters
Index simpleIndex = Index.of('email', null, 'email-index')

// Create index with full control over key types
Index customIndex = Index.of(
  Scalar.of('timestamp', ScalarAttributeType.N),
  Scalar.of('status', ScalarAttributeType.S),
  'timestamp-status-index'
)

// Auto-generated index name (will be 'email-index')
Index autoNamedIndex = Index.of('email')

// Use Keys class for cleaner index definition
Keys indexKeys = Keys.of('email', 'status')
Index combinedIndex = new Index(indexKeys, 'email-status-index')

// Use in table creation
dynamoDb.createTable(
  'users',
  Scalar.of('userId'),
  Optional.of(Scalar.of('createdAt')),
  [
    Index.of('email'),
    Index.of('status', 'createdAt')
  ] as Index[]
)
```

### Batch Operations

Store multiple items at once:

```groovy
def users = [
  new User(id: 'user1', username: 'john', email: 'john@example.com'),
  new User(id: 'user2', username: 'jane', email: 'jane@example.com')
]

dynamoDb.putItems('users', users)
```

Delete multiple items in a batch:

```groovy
// Delete items by partition key
int deleted = dynamoDb.deleteItems (
  'users',
  KeyFilter.of('status', 'inactive')
)

// Delete items by partition key with additional filter
int deleted = dynamoDb.deleteItems (
  'users',
  KeyFilter.of('status', 'inactive'), 
  match('lastLogin', '2022-01-01')
)

// Delete items using an index
int deleted = dynamoDb.deleteItems(
  'users',
  'email-index', 
  KeyFilter.of('domain', 'example.com'), 
  match('active', false)
)

// Delete items by scanning the entire table with a filter
int deleted = dynamoDb.deleteItems (
  'users', 
   every (
    match('status', 'deleted'),
    less('lastAccess', '2023-01-01')
   )
)
```

### Pagination Support

The library offers two pagination approaches using the same `query()` method name, distinguished by return type and parameters:

#### Automatic Pagination (Returns List<T>)

Use `query()` methods without a `limit` parameter to automatically retrieve **all** results. The library handles pagination internally by calling DynamoDB multiple times until all data is retrieved:

```groovy
// Automatically retrieves ALL results - no pagination handling needed
List<User> allUsers = dynamoDb.query (
  'users',
  KeyFilter.of('status', 'active'),
  filters,
  User.class
)

// Works with indexes too - gets ALL matching results
List<User> allActiveUsers = dynamoDb.query (
  'users',
  'status-index',
  KeyFilter.of('status', 'active'),
  filters,
  User.class
)

// Perfect for small to medium result sets where you need all data
List<User> recentUsers = dynamoDb.query (
  'users',
  KeyFilter.of (
    'userId', 'user123',
    greater('timestamp', yesterdayTimestamp)
  ),
  User.class
)
```

#### Manual Pagination Control (Returns PagedResult<T>)

Use `query()` methods with a `limit` parameter when you need fine-grained pagination control:

```groovy
// Query with limit for manual pagination
PagedResult<User> page1 = dynamoDb.query (
  'users',
  KeyFilter.of('userId', 'user123'),
  filters,
  User.class,
  5     // Limit to 5 items per page
)

// Access results
page1.items    // List of User objects in this page
page1.more     // true if more results available
page1.last     // Last evaluated key for next page
page1.count    // Number of items in this page (5 or fewer)

// Get next page if more results exist
if (page1.more) {
  PagedResult<User> page2 = dynamoDb.query (
    'users',
    KeyFilter.of('userId', 'user123'),
    filters,
    User.class,
    5,          // Limit
    page1.last  // Last evaluated key from previous page
  )
}

// Continue until no more pages
def allPages = []
PagedResult<User> currentPage = page1
while (currentPage != null) {
  allPages.addAll(currentPage.items)
  if (currentPage.more) {
    currentPage = dynamoDb.query (
      'users',
      KeyFilter.of('userId', 'user123'),
      filters,
      User.class,
      5,
      currentPage.last
    )
  } else {
    currentPage = null
  }
}
```

#### When to Use Each Approach

**Use Automatic Pagination (`query()` returning `List<T>`) when:**
- You need all results from a query
- Working with small to medium result sets (up to a few thousand items)
- Simplicity is preferred over performance optimization
- You don't need to display results incrementally

**Use Manual Pagination (`query()` with `limit` returning `PagedResult<T>`) when:**
- Working with large result sets that might cause memory issues
- Implementing UI pagination (showing page 1, 2, 3, etc.)
- You need to process results in chunks
- Performance optimization is critical
- You want to limit the number of DynamoDB requests

#### Common Pagination Patterns

```groovy
// The key difference: presence of limit parameter determines return type
// No limit = List<T> (automatic pagination, gets all results)
// With limit = PagedResult<T> (manual pagination, controlled chunks)

// Pattern 1: Load all for dropdown/selection lists
List<Category> allCategories = dynamoDb.query(
  'categories',
  KeyFilter.of('type', 'active'),
  Category.class  // No limit = gets ALL categories automatically
)

// Pattern 2: UI pagination for large datasets  
PagedResult<Order> ordersPage = dynamoDb.query(
  'orders',
  'customer-index',
  KeyFilter.of('customerId', customerId),
  Order.class,
  pageSize  // With limit = gets PagedResult for manual control
)

// Pattern 3: Processing large datasets in chunks
PagedResult<Transaction> transactions = null
Map<String, AttributeValue> lastKey = null
do {
  transactions = dynamoDb.query(
    'transactions',
    KeyFilter.of('accountId', accountId),
    null,
    Transaction.class, 
    100,  // Process 100 at a time
    lastKey
  )
  
  // Process this batch
  processTransactions(transactions.items)
  lastKey = transactions.last
  
} while (transactions.more)
```

### Query Ordering

Control the ordering of query results using the forward parameter:

```groovy
// Query with ascending order (default)
List<Item> ascending = dynamoDb.query (
  'users',
  KeyFilter.of('id', 'user1'),
  filters,
  Item.class,
  true     // Forward order (oldest to newest if sort key is timestamp)
)

// Query with descending order
List<Item> descending = dynamoDb.query (
  'users',
  KeyFilter.of('id', 'user1'),
  filters,
  Item.class,
  false        // Backward order (newest to oldest if sort key is timestamp)
)

// Complete query method with all parameters
PagedResult<Item> result = dynamoDb.query (
  'users',               // Table name
  'email-index',         // Index name (optional)
  KeyFilter.of (          // Key condition
    'email',
    'test@example.com'
  ),
  match('active', true), // Filter expression (optional)
  Item.class,            // Target class
  10,                    // Limit (for pagination)
  null,                  // Last evaluated key from previous query
  false                  // Backward order (newest first)
)
```

### Field Projection

Control which fields are retrieved from DynamoDB to reduce data transfer and improve performance:

```groovy
// GetItem with projection - retrieve only specific fields
DynamoMap user = dynamoDb.getItem (
  'users',
  KeyFilter.of('id', 'user123'),
  ['id', 'email', 'username'],  // Only retrieve these fields
  DynamoMap
)

// Fields not in projection will be null
assert user.id != null
assert user.email != null
assert user.username != null
assert user.address == null  // Not projected, so null

// GetItem with projection - custom target class
User partialUser = dynamoDb.getItem (
  'users',
  KeyFilter.of('id', 'user123'),
  ['id', 'email'],  // Minimal field set
  User
)

// Query with projection - automatic pagination
List<DynamoMap> users = dynamoDb.query (
  'users',
  KeyFilter.of('status', 'active'),
  ['id', 'email', 'lastLogin'],  // Project specific fields
  null,  // No additional filter
  DynamoMap
)

// Query with projection and filters
List<DynamoMap> recentUsers = dynamoDb.query (
  'users',
  KeyFilter.of('department', 'engineering'),
  ['id', 'name', 'role'],  // Projection fields
  greater('lastLogin', yesterdayTimestamp),  // Additional filter
  DynamoMap
)

// Query index with projection
List<DynamoMap> emailUsers = dynamoDb.query (
  'users',
  'email-index',
  KeyFilter.of('domain', 'company.com'),
  ['id', 'email', 'department'],  // Only these fields
  null,  // No filter
  DynamoMap
)

// Query with projection and manual pagination
PagedResult<DynamoMap> pagedUsers = dynamoDb.query (
  'users',
  'status-index',
  KeyFilter.of('status', 'active'),
  ['id', 'username', 'lastSeen'],  // Projected fields
  null,      // No filter
  DynamoMap,
  10,        // Page size
  null,      // Start from beginning
  true       // Forward order
)

// Complex query with projection, index, filter, and pagination
PagedResult<DynamoMap> complexQuery = dynamoDb.query (
  'users',
  'department-index',
  KeyFilter.of('department', 'sales'),
  ['id', 'name', 'email', 'performance'],  // Projection
  every (
    greater('lastLogin', cutoffDate),
    match('active', true)
  ),         // Filter
  DynamoMap,
  20,        // Limit
  lastKey,   // Continue from previous page
  false      // Reverse order
)

// Scan with projection - retrieve only specific fields
List<DynamoMap> allUsers = dynamoDb.scan (
  'users',
  ['id', 'email', 'lastLogin']  // Only these fields
)

// Scan with filter and projection
List<DynamoMap> activeUsers = dynamoDb.scan (
  'users',
  match('status', 'active'),     // Filter condition
  ['id', 'username', 'department'],  // Projected fields
  DynamoMap
)

// Scan with projection using simplified interface  
List<DynamoMap> userList = dynamoDb.scan (
  'users',
  ['id', 'name']  // Minimal projection for lists
)

// Full-featured scan with projection and advanced options
List<DynamoMap> departmentScan = dynamoDb.scan (
  'users',
  match('department', 'engineering'),  // Filter
  ['id', 'name', 'role', 'level'],     // Projection
  DynamoMap,                           // Target class
  100                                  // Evaluation limit
)
```

**Projection Benefits:**
- **Reduced Data Transfer**: Only requested fields are returned, reducing network traffic
- **Lower Costs**: Fewer consumed read capacity units when projecting smaller field sets
- **Improved Performance**: Smaller payloads mean faster response times
- **Security**: Sensitive fields can be excluded from queries

**Important Notes:**
- Projection works with `getItem()`, all `query()` method variants, and all `scan()` method variants
- Non-projected fields will be `null` in the returned objects
- Key fields (partition key, sort key) are always included regardless of projection
- All existing method signatures remain unchanged for backward compatibility
- Projection is optional - omit the projection parameter to retrieve all fields

**Use Cases:**
- **List Views**: Retrieve only display fields for user lists (`['id', 'name', 'email']`)
- **Search Results**: Show minimal info in search results (`['id', 'title', 'summary']`)
- **API Responses**: Control which fields are exposed in different API endpoints
- **Performance Optimization**: Reduce payload size for frequently accessed queries
- **Security**: Exclude sensitive fields from certain operations

### Removing Attributes

Remove specific attributes from existing items without affecting other fields:

```groovy
// Remove single attribute
dynamoDb.removeAttributes (
  'users',
  KeyFilter.of('id', 'user123'),
  'temporaryField'
)

// Remove multiple attributes at once
dynamoDb.removeAttributes (
  'users', 
  KeyFilter.of('id', 'user123'),
  'oldField1', 'oldField2', 'deprecatedData'
)

// Works with composite keys
dynamoDb.removeAttributes (
  'userEvents',
  KeyFilter.of('userId', 'user123', 'timestamp', 1642681200),
  'cachedData', 'processedFlag'
)

// Use in combination with updates via DynamoMapper
DynamoMapper mapper = new DynamoMapper()
  .with('id', 'user123', FieldType.PARTITION_KEY)
  .with('status', 'updated')           // SET operation
  .remove('temporaryField', 'cache')   // REMOVE operation
  
dynamoDb.updateItem('users', mapper)
```

**Important Notes:**
- Key attributes (partition key, sort key) cannot be removed and will throw a `DynamoDbException` if specified
- Non-existent attributes are silently ignored without causing errors
- Remove operations are atomic and can be combined with SET operations in update expressions
- The operation preserves versioning if enabled on the item

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

## Contributing

Contributions are welcome! See the repository for more details.
