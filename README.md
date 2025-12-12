<p align="center">
  <img src="resources/logo.png" alt="Impress Logo" width="400"/>
</p>

# Impress 🚀

> **The most intuitive DynamoDB library for Groovy developers**

Transform your DynamoDB interactions from complex AWS SDK calls into elegant, type-safe Groovy code. Impress leverages the impression pattern to provide seamless object persistence with powerful querying capabilities.

## ✨ Why Choose Impress?

- **🎯 Developer-First Design**: Intuitive APIs that feel natural in Groovy
- **⚡ Type-Safe Operations**: Compile-time checking with full IDE support
- **🔧 Zero Configuration**: Start coding immediately with sensible defaults
- **📊 Advanced Querying**: Complex filters with nested field support
- **🚄 Performance Optimized**: Built-in pagination, projection, and batch operations
- **🛡️ Production Ready**: Optimistic locking, transactions, and error handling

## 🚀 Quick Start

Get up and running in under 2 minutes:

### Installation

**Gradle:**
```groovy
repositories {
  maven { url 'https://jitpack.io' }
}

dependencies {
  implementation 'com.github.grational:impress:latest.release'
}
```

**Maven:**
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

### Hello World Example

```groovy
import it.grational.storage.dynamodb.*
import static it.grational.storage.dynamodb.DynamoFilter.*

// 1. Connect to DynamoDB
def dynamo = new DynamoDb()

// 2. Create a table instantly
dynamo.createTable('users', // table name
  'id',                     // table S partition key
  Index.of('email')         // index on email field called email-index
                            // alternative: [ 'email-index': 'email' ]
)

// 3. Save data - works with any Map or custom object
dynamo.putItem('users',
  new DynamoMap ( // object with native impression support
    id: 'user123',
    name: 'Alice Johnson',
    email: 'alice@example.com',
    profile: [
      department: 'Engineering',
      skills: ['Groovy', 'DynamoDB', 'AWS']
    ]
  )
)

// 4. Retrieve with type safety using builder pattern
DynamoMap user = dynamo.getItem('users', KeyFilter.of('id', 'user123'))
  .get()
println "Welcome ${user.name}!" // Direct field access

// 5. Query with powerful filters using builder pattern
List<DynamoMap> engineers = dynamo.scan('users')
  .filter(match('profile.department', 'Engineering'))
  .list()

// 6. Complex queries made simple with fluent API
List<DynamoMap> seniorDevs = dynamo.scan('users')
  .filter(every(
    match('profile.department', 'Engineering'),
    contains('profile.skills', 'Groovy')
  ))
  .list()
```

That's it! You're now managing DynamoDB data with elegant Groovy syntax.

## 🎯 What's New

### 🎯 **Smart Result Limiting with `take()`** (Latest)
Intuitive result limiting that's distinct from pagination control. Unlike `limit()` which controls page size, `take()` controls the total number of results returned:

```groovy
// Limit total results across all pages (new take() method)
List<User> topUsers = dynamo.scan('users', User)
  .filter(match('status', 'premium'))
  .take(10)  // Get exactly 10 results total
  .list()

// Query with take() - perfect for "top N" scenarios
List<Order> recentOrders = dynamo.query (
    'orders',
    KeyFilter.of('customerId', 'cust1'),
    Order
  )
  .backward()  // newest first
  .take(5)     // get last 5 orders
  .list()

// Works with all builder features
List<Product> featured = dynamo.scan('products')
  .filter(match('featured', true))
  .fields('id', 'name', 'price')
  .take(20)  // Top 20 featured products
  .list()
```

### 🏗️ **Builder Pattern for Query & Scan** 
Revolutionary fluent API using builder pattern for constructing queries and scans. Chain operations naturally with `.filter()`, `.fields()`, `.take()`, and execute with `.list()` or `.paged()`:

```groovy
// Before: Multiple parameters in specific order
List<User> users = dynamo.scan('users', filter, fields, User, limit)

// Now: Fluent builder pattern - readable and flexible
List<User> users = dynamo.scan('users', User)
  .filter(match('status', 'active'))
  .fields('id', 'name', 'email')
  .take(50)  // Limit results, not page size
  .list()

// Query with index and pagination
PagedResult<Order> orders = dynamo.query (
    'orders',
    'status-index',
    keyFilter,
    Order
  )
  .filter(match('status', 'shipped'))
  .paged(50)
```

### 🔑 **DynamoMap Key Specification**
Enhanced DynamoMap with seamless key specification for putItem operations - making it even easier to work with dynamic data structures.

### 📊 **Enhanced Projection Support**
Reduce bandwidth and costs by retrieving only the fields you need:

```groovy
// Get only essential fields using builder pattern
DynamoMap user = dynamo.getItem('users', KeyFilter.of('id', 'user123'))
  .fields('id', 'name', 'email')  // 70% less data transfer!
  .get()
```

### 📄 **Intelligent Pagination**
Two pagination approaches - automatic for simplicity, manual for control:

```groovy
// Automatic: Get ALL results (perfect for small-medium datasets)
List<DynamoMap> allUsers = dynamo.query (
    'users',
    KeyFilter.of('status', 'active')
  ).list()

// Manual: Fine-grained control (perfect for UI pagination)
PagedResult<DynamoMap> page = dynamo.query (
    'users',
    KeyFilter.of('status', 'active')
  ).paged(10)
```

## 🏗️ Core Architecture

### The Storable Pattern

Every persistable object implements the lightweight `Storable` interface:

```groovy
interface Storable<S,B> {
  DbMapper<S,B> impress(DbMapper<S,B> mapper, boolean versioned)
}
```

This elegant pattern provides:
- **Automatic serialization** to DynamoDB format
- **Type safety** with compile-time checking
- **Version management** for optimistic locking
- **Flexible mapping** for complex nested objects

### Domain Objects Made Simple

```groovy
import it.grational.storage.dynamodb.*
import static it.grational.storage.dynamodb.FieldType.*

class User extends Dynable {
  String id, username, email
  Integer loginCount = 0

  User() {}
  User(Map<String, Object> data) {
    this.id = data.id
    this.username = data.username
    this.email = data.email
    this.loginCount = data.loginCount ?: 0
  }

  @Override
  protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
    return mapper
      .with('id', id, PARTITION_KEY)
      .with('username', username)
      .with('email', email)
      .with('loginCount', loginCount)
  }

  @Override
  KeyFilter key() {
    return KeyFilter.of('id', id)
  }
}

// Use your domain object
def user = new User(id: 'u1', username: 'alice', email: 'alice@example.com')
dynamo.putItem('users', user)

// Retrieve with full type safety using builder pattern
User retrievedUser = dynamo.getItem('users', KeyFilter.of('id', 'u1'), User)
  .get()
```

## 🔍 Advanced Querying

### Powerful Filtering Engine

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

// Basic conditions with builder pattern
def activeUsers = dynamo.scan('users')
  .filter(match('status', 'active'))
  .take(100)  // Limit results
  .list()
def seniorUsers = dynamo.scan('users')
  .filter(greater('experience', 5))
  .take(50)   // Top 50 senior users
  .list()

// String operations
def developers = dynamo.scan('users')
  .filter(contains('bio', 'developer'))
  .take(30)   // First 30 developers
  .list()
def managers = dynamo.scan('users')
  .filter(beginsWith('title', 'Senior'))
  .take(20)   // Top 20 senior managers
  .list()

// Complex combinations with fluent chaining
def targetUsers = dynamo.scan('users')
  .filter(every(
    match('department', 'Engineering'),
    any(
      greater('experience', 3),
      contains('skills', 'leadership')
    ),
    defined('email')
  ))
  .take(15)   // Top 15 target users
  .list()

// Nested field queries with dot notation
def premiumUsers = dynamo.scan('users')
  .filter(every(
    match('subscription.tier', 'premium'),
    greater('subscription.usage.apiCalls', 1000),
    match('profile.preferences.notifications', true)
  ))
  .take(10)   // Top 10 premium users
  .list()
```

### Smart Key Conditions

```groovy
// Simple keys
KeyFilter userKey = KeyFilter.of('userId', 'user123')

// Composite keys
KeyFilter orderKey = KeyFilter.of('customerId', 'cust1', 'orderId', 'ord456')

// Range conditions for queries
KeyFilter recentOrders = KeyFilter.of(
  'customerId', 'cust1',
  greater('timestamp', yesterday)
)
List<DynamoMap> orders = dynamo.query (
    'orders',
    recentOrders
  )
  .take(20)  // Latest 20 orders
  .list()

// Multiple data types supported
KeyFilter mixedKey = KeyFilter.of(
  'stringId', 'abc123',
  'numericSort', 42.5,
  'binaryData', byteArray
)
```

## 📊 Performance & Optimization

### Field Projection
Reduce costs and improve performance by retrieving only necessary fields:

```groovy
// Get minimal user info for listings with builder pattern
List<DynamoMap> userList = dynamo.scan('users')
  .fields('id', 'name', 'avatar')  // 90% smaller payload
  .take(50)  // Limit to first 50 users
  .list()

// Query with projection and filtering using fluent API
List<DynamoMap> activeEngineers = dynamo.scan('users')
  .filter(match('department', 'Engineering'))
  .fields('id', 'name', 'skills', 'level')
  .take(25)  // Top 25 engineers
  .list()

// Works with all operations using builder pattern
DynamoMap profile = dynamo.getItem('users', KeyFilter.of('id', 'user123'))
  .fields('name', 'email', 'lastLogin')  // Minimal profile
  .get()
```

### Intelligent Batching

```groovy
// Batch writes (up to 25 items automatically)
def users = [
  new User(id: 'u1', username: 'alice'),
  new User(id: 'u2', username: 'bob'),
  // ... up to 25 items
]
dynamo.putItems('users', users)  // Single batch operation

// Batch deletes with filters
int deleted = dynamo.deleteItems('users',
  KeyFilter.of('status', 'inactive'),
  less('lastLogin', cutoffDate)
)
```

### Memory-Efficient Pagination

```groovy
// Process large datasets without memory issues using builder pattern
PagedResult<Order> orders = null
Map<String, AttributeValue> lastKey = null

do {
  orders = dynamo.query (
    'orders',
    KeyFilter.of('customerId', customerId),
    Order
  )
  .paged(100, lastKey)  // page size and continuation

  processOrders(orders.items)  // Process 100 items at a time
  lastKey = orders.last

} while (orders.more)
```

## 🏢 Enterprise Features

### Optimistic Locking
Built-in versioning prevents concurrent modification issues:

```groovy
// Automatic versioning (default)
dynamo.putItem('users', user, true)

// Manual version control
user.v = 5  // Set specific version
dynamo.putItem('users', user)  // Will fail if DB version != 5
```

### Schema Management

```groovy
// Simple table creation
dynamo.createTable('users', 'id')

// With sort key and indexes
dynamo.createTable('orders',
  'customerId',     // Partition key
  'timestamp',      // Sort key
  Index.of('status'),            // Auto-named: status-index
  Index.of('product', 'region')  // Composite index
)

// Advanced schema with types
dynamo.createTable('metrics',
  Scalar.of('metricId', ScalarAttributeType.S),
  Scalar.of('timestamp', ScalarAttributeType.N),
  Index.of(
    Scalar.of('category', ScalarAttributeType.S),
    Scalar.of('value', ScalarAttributeType.N),
    'category-value-index'
  )
)
```

### Flexible Data Access

DynamoMap provides both Map-like operations and direct field access:

```groovy
DynamoMap user = dynamo.getItem('users', KeyFilter.of('id', 'user123'))
  .get()

// Direct field access (via @Delegate)
String name = user.name
String email = user.email
Long timestamp = user.timestamp

// Map operations
user.each { key, value ->
  println "$key: $value"
}

// Nested access
String city = user.address?.city
Boolean premium = user.subscription?.isPremium
```

## 🛠️ Development & Testing

### Local Development
Perfect for testing with DynamoDB Local:

```groovy
import software.amazon.awssdk.auth.credentials.*
import java.net.URI

// Connect to local DynamoDB instance
def localClient = DynamoDbClient.builder()
  .endpointOverride('http://localhost:8000'.toURI())
  .credentialsProvider(StaticCredentialsProvider.create(
    AwsBasicCredentials.create('dummy', 'dummy')
  ))
  .build()

def dynamo = new DynamoDb(localClient)
```

### Testing Best Practices

```groovy
// Clean setup for each test
class UserServiceSpec extends Specification {

  def dynamo = new DynamoDb()

  def setup() {
    dynamo.createTable('users', 'id', ['email-index': 'email'])
  }

  def cleanup() {
    dynamo.dropTable('users')
  }

  def "should find users by department"() {
    given:
    dynamo.putItem('users', [id: '1', department: 'Engineering'])
    dynamo.putItem('users', [id: '2', department: 'Sales'])

    when:
    def engineers = dynamo.scan('users')
      .filter(match('department', 'Engineering'))
      .list()

    then:
    engineers.size() == 1
    engineers[0].department == 'Engineering'
  }
}
```

## 📚 Comprehensive Examples

### E-Commerce Application

```groovy
// Product catalog
class Product extends Dynable {
  String productId, name, category
  BigDecimal price
  List<String> tags
  Map<String, Object> specs

  // ... constructor and impress() implementation
}

// Order management
class Order extends Dynable {
  String orderId, customerId
  Long timestamp
  List<Map> items
  String status

  // ... implementation
}

// Complex queries with builder pattern
def featured = dynamo.scan('products')
  .filter(every(
    match('category', 'electronics'),
    contains('tags', 'featured'),
    between('price', 100, 1000)
  ))
  .take(25)  // Top 25 featured products
  .list()

def recentOrders = dynamo.query (
  'orders',
  KeyFilter.of(
    'customerId', 'cust123',
    greater('timestamp', lastWeek)
  ),
  Order
)
.filter(match('status', 'completed'))
.take(10)  // Latest 10 completed orders
.list()
```

### User Analytics

```groovy
// Track user activities with nested data
def activity = new DynamoMap(
  userId: 'user123',
  timestamp: System.currentTimeMillis(),
  action: 'page_view',
  metadata: [
    page: '/dashboard',
    duration: 45,
    referrer: 'google.com',
    device: [
      type: 'mobile',
      os: 'iOS',
      browser: 'Safari'
    ]
  ]
)
dynamo.putItem('activities', activity)

// Query with nested conditions using builder pattern
def mobileSafariUsers = dynamo.scan('activities')
  .filter(every(
    match('metadata.device.type', 'mobile'),
    match('metadata.device.browser', 'Safari'),
    greater('metadata.duration', 30)
  ))
  .take(100)  // Top 100 mobile Safari users
  .list()
```

## 🚦 Migration Guide

### From AWS SDK v2

**Before (AWS SDK):**
```java
// Verbose and error-prone
Map<String, AttributeValue> key = new HashMap<>();
key.put("id", AttributeValue.builder().s("user123").build());

GetItemRequest request = GetItemRequest.builder()
  .tableName("users")
  .key(key)
  .build();

GetItemResponse response = dynamoDbClient.getItem(request);
// Complex parsing required...
```

**After (Impress):**
```groovy
// Clean and intuitive
DynamoMap user = dynamo.getItem('users', KeyFilter.of('id', 'user123'))
println user.name  // Direct access
```

### From Other DynamoDB Libraries

Impress provides migration helpers and multiple API styles to ease transitions from other libraries while providing superior type safety and Groovy integration.

## 🔧 Configuration

### AWS Credentials
Impress uses standard AWS credential providers:

```groovy
// Default credential chain (recommended)
def dynamo = new DynamoDb()

// Custom credentials
def credentials = StaticCredentialsProvider.create(
  AwsBasicCredentials.create(accessKey, secretKey)
)

def customClient = DynamoDbClient.builder()
  .credentialsProvider(credentials)
  .region(Region.US_EAST_1)
  .build()

def dynamo = new DynamoDb(customClient)
```

### Performance Tuning

```groovy
// Connection pooling (built-in)
def client = DynamoDbClient.builder()
  .overrideConfiguration(ClientOverrideConfiguration.builder()
    .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, "Impress")
    .build())
  .build()

// Batch size optimization
dynamo.putItems('large_table', items, 25)  // Max batch size
```

## 🌟 Advanced Patterns

### Repository Pattern

```groovy
class UserRepository {
  private final DynamoDb dynamo
  private final String tableName = 'users'

  UserRepository(DynamoDb dynamo) {
    this.dynamo = dynamo
  }

  User findById(String id) {
    return dynamo.getItem(tableName, KeyFilter.of('id', id), User)
      .get()
  }

  List<User> findByDepartment(String department) {
    return dynamo.scan(tableName, User)
      .filter(match('department', department))
      .list()
  }

  List<User> findTopActiveUsers(int count) {
    return dynamo.scan(tableName, User)
      .filter(match('status', 'active'))
      .take(count)  // Limit results using take()
      .list()
  }

  PagedResult<User> findActiveUsersPaged(int limit, Map<String, AttributeValue> lastKey = null) {
    return dynamo.query(tableName, 'status-index', KeyFilter.of('status', 'active'), User)
      .paged(limit, lastKey)
  }

  void save(User user) {
    dynamo.putItem(tableName, user)
  }
}
```

### Event Sourcing

```groovy
class EventStore {
  private final DynamoDb dynamo

  void appendEvent(String streamId, Object event) {
    def eventRecord = new DynamoMap(
      streamId: streamId,
      timestamp: System.currentTimeMillis(),
      eventType: event.class.simpleName,
      data: objectToMap(event),
      version: getNextVersion(streamId)
    )

    dynamo.putItem('events', eventRecord)
  }

  List<DynamoMap> getEventStream(String streamId, Long fromTimestamp = null) {
    def keyCondition = fromTimestamp ?
      KeyFilter.of('streamId', streamId, greater('timestamp', fromTimestamp)) :
      KeyFilter.of('streamId', streamId)

    return dynamo.query('events', keyCondition)
      .list()
  }
}
```

## 🔄 Compatibility

- **Main Branch**: Java 21+ and Groovy 4.x (Recommended for new projects)
- **Legacy Support**: Java 8 and Groovy 3.x compatible version available on `java-8-groovy-3` branch (released as `$version-j8g3`)

## 🐛 Troubleshooting

### Common Issues

**Connection Problems:**
```groovy
// Verify AWS credentials
def dynamo = new DynamoDb()
try {
  dynamo.scan('non-existent-table')
} catch (Exception e) {
  println "AWS Setup Issue: ${e.message}"
}
```

**Performance Optimization:**
```groovy
// Use projection for large objects with builder pattern
def lightweightData = dynamo.scan('heavy_table')
  .fields('id', 'name')  // Only essential fields
  .take(100)  // Limit results to reduce load
  .list()

// Batch operations for bulk work
dynamo.putItems('bulk_table', largeDataset.collate(25))
```

**Memory Management:**
```groovy
// Use pagination for large result sets with builder pattern
def processLargeTable() {
  PagedResult<DynamoMap> page = null
  Map<String, AttributeValue> lastKey = null

  do {
    page = dynamo.scan('large_table')
      .paged(100, lastKey)
    processPage(page.items)
    lastKey = page.last
  } while (page.more)
}
```

## 🤝 Contributing

We welcome contributions! Impress is built with ❤️ by the Groovy community.

**Development Setup:**
```bash
git clone https://github.com/grational/impress.git
cd impress
./gradlew test
```

**Testing:**
- Unit tests: `./gradlew uniTest`
- Integration tests: `./gradlew intTest` (requires DynamoDB Local)
- All tests: `./gradlew test`

## 📄 License

Impress is open source software released under the MIT License.

---

<p align="center">
  <strong>Ready to impress your data?</strong><br>
  ⭐ Star us on GitHub | 📚 Read the docs | 💬 Join our community
</p>

## 📖 API Reference

### Core Classes Quick Reference

| Class | Purpose | Key Methods |
|-------|---------|-------------|
| `DynamoDb` | Main API | `getItem()`, `putItem()`, `query()`, `scan()` (returns builders) |
| `GetItemBuilder` | Fluent getItem building | `fields()`, `as()`, `get()` |
| `QueryBuilder` | Fluent query building | `filter()`, `fields()`, `take()`, `backward()`, `list()`, `paged()` |
| `ScanBuilder` | Fluent scan building | `filter()`, `fields()`, `take()`, `limit()`, `segment()`, `list()`, `paged()` |
| `KeyFilter` | Key conditions | `of()`, `partition()`, `sort()` |
| `DynamoFilter` | Query filters | `match()`, `greater()`, `contains()`, `every()`, `any()` |
| `DynamoMap` | Flexible data container | Direct field access via `@Delegate`, `withKeys()` |
| `Index` | Schema definition | `of()` with auto-naming |
| `PagedResult` | Pagination wrapper | `.items`, `.more`, `.last`, `.count` |

### Static Import Recommendations

```groovy
// Essential imports for fluent API
import static it.grational.storage.dynamodb.DynamoFilter.*
import static it.grational.storage.dynamodb.FieldType.*

// Usage becomes very clean with builder pattern
def results = dynamo.scan('users')
  .filter(every(
    match('status', 'active'),
    greater('lastLogin', cutoff)
  ))
  .take(50)  // Top 50 active users
  .list()
```

### Builder Pattern Methods Summary

All builder methods return the builder instance for chaining:

**GetItemBuilder:**
```groovy
dynamo.getItem(table, keyFilter, [Type])
  .fields('field1', 'field2')     // Project specific fields
  .as(MyClass)                    // Specify return type 
  .get()                          // Execute and return result
```

**QueryBuilder:**
```groovy
dynamo.query(table, keyFilter, [Type])
  .filter(dynamoFilter)           // Add filter conditions
  .fields('field1', 'field2')     // Project specific fields
  .as(MyClass)                    // Specify return type
  .take(100)                      // Limit total results returned
  .forward()/backward()           // Sort direction
  .list()                         // Get all results as List<T>
  .paged(pageSize, lastKey)       // Get paginated results
```

**ScanBuilder:**
```groovy
dynamo.scan(table, [Type])
  .filter(dynamoFilter)           // Add filter conditions  
  .fields('field1', 'field2')     // Project specific fields
  .as(MyClass)                    // Specify return type
  .take(100)                      // Limit total results returned
  .limit(50)                      // Page size limit
  .segment(0, 4)                  // Parallel scan segment
  .list()                         // Get all results as List<T>
  .paged(pageSize, lastKey)       // Get paginated results
```

**Key Differences:**
- `take()` - Controls total number of results across all pages
- `limit()` - Controls DynamoDB page size (ScanBuilder only)
- `list()` - Returns all results automatically handling pagination
- `paged()` - Returns single page with continuation token

---

*Built with ❤️ for the Groovy and AWS communities*
