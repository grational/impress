# Impress

A Groovy library to store your objects into any storage, with a concrete implementation for AWS DynamoDB.

## Overview

Impress provides a simple, flexible way to serialize your Groovy objects to different storage backends. The library uses a mapping approach that converts your domain objects to a format suitable for storage and vice versa.

The library currently includes:
- Core interfaces for defining storage operations
- A complete implementation for AWS DynamoDB

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

The `Storable<S,B>` interface is the foundation of the library. Any class that needs to be stored must implement this interface.

- `S`: The storage-specific type (e.g., AttributeValue for DynamoDB)
- `B`: The builder type used for reconstruction (typically Object)

```groovy
interface Storable<S,B> {
    DbMapper<S,B> impress(DbMapper<S,B> mapper, boolean versioned)
}
```

### DbMapper

The `DbMapper<S,B>` interface defines how to map fields to storage formats:

```groovy
interface DbMapper<S,B> {
    DbMapper<S,B> with(String k, String s)
    DbMapper<S,B> with(String k, Number n)
    DbMapper<S,B> with(String k, boolean b)
    DbMapper<S,B> with(String k, DbMapper<S,B> dm, boolean version)
    DbMapper<S,B> with(String k, List<String> ls)
    Map<String,S> storer(boolean version)
    Map<String,B> builder(boolean version)
}
```

## DynamoDB Implementation

### Dynable

When working with DynamoDB, your domain objects should extend the `Dynable` abstract class, which implements the `Storable` interface for DynamoDB's `AttributeValue` type.

```groovy
abstract class Dynable implements Storable<AttributeValue,Object> {
    // Provides versioning support
    protected Integer v = 0
    
    // Abstract methods to implement
    protected abstract DbMapper<AttributeValue,Object> inpress(DynamoMapper mapper)
    abstract DynamoKey key()
}
```

### DynamoDb

The `DynamoDb` class provides the API for interacting with DynamoDB tables:

```groovy
class DynamoDb {
    // Constructor with AWS DynamoDB client
    DynamoDb(DynamoDbClient client)
    
    // Default constructor
    DynamoDb()
    
    // Store a single item
    void putItem(String table, Storable<AttributeValue,Object> storable, boolean versioned = true)
    
    // Store multiple items
    void putItems(String table, List<? extends Storable<AttributeValue,Object>> storables, boolean versioned = true)
    
    // Update an existing item
    UpdateItemResponse updateItem(String table, DynamoMapper mapper)
    
    // Retrieve an object by key
    <T extends Storable<AttributeValue,Object>> T objectByKey(String table, DynamoKey key, Class<T> targetClass)
    
    // Query objects by index
    <T extends Storable<AttributeValue,Object>> List<T> objectsByIndex(String table, String index, DynamoKey key, Class<T> targetClass, DynamoFilter filter = null)
    
    // Delete an item
    DeleteItemResponse deleteItem(String table, DynamoKey key)
    
    // Create a new table
    void createTable(String table, String partitionKey, String sortKey = null, Map<String, String> indexes = [:])
    
    // Delete a table
    void dropTable(String table)
}
```

### DynamoKey

Used to define partition and sort keys for DynamoDB operations:

```groovy
// Simple partition key
DynamoKey key = new DynamoKey("id", "abc123")

// Composite key (partition + sort)
DynamoKey compositeKey = new DynamoKey("userId", "user1", "timestamp", 1234567890)
```

### DynamoFilter

Provides a fluent API for building DynamoDB filter expressions:

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

// Create filters
def activeFilter = equals("status", "active")
def highPriorityFilter = greaterThan("priority", 7)

// Combine filters
def combinedFilter = activeFilter.and(highPriorityFilter)

// Use with queries
List<Task> tasks = dynamoDb.objectsByIndex(
    "tasks",
    "status-index",
    new DynamoKey("status", "pending"),
    Task.class,
    combinedFilter
)
```

## Usage Examples

### Creating a Domain Object

```groovy
import it.grational.storage.dynamodb.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static it.grational.storage.dynamodb.FieldType.*

class User extends Dynable {
    String id
    String username
    String email
    boolean active
    List<String> roles
    Map<String, Object> metadata
    
    User() {}
    
    User(Map<String, Object> builder) {
        super(builder)
        this.id = builder.id
        this.username = builder.username
        this.email = builder.email
        this.active = builder.active ?: false
        this.roles = builder.roles ?: []
        this.metadata = builder.metadata ?: [:]
    }
    
    @Override
    protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
        return mapper
            .with('id', id, PARTITION_KEY)
            .with('username', username)
            .with('email', email)
            .with('active', active)
            .with('roles', roles)
    }
    
    @Override
    DynamoKey key() {
        return new DynamoKey('id', id)
    }
}
```

### Basic CRUD Operations

```groovy
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import it.grational.storage.dynamodb.*

// Initialize DynamoDB client
def dynamoDb = new DynamoDb()

// Create a table
dynamoDb.createTable(
    "users",
    "id",
    null,
    ["username-index": "username", "email-index": "email"]
)

// Create a user
def user = new User(
    id: 'user-123',
    username: 'johndoe',
    email: 'john@example.com',
    active: true,
    roles: ['user', 'admin']
)

// Save the user
dynamoDb.putItem("users", user)

// Retrieve the user by primary key
User retrievedUser = dynamoDb.objectByKey(
    "users",
    new DynamoKey("id", "user-123"),
    User.class
)

// Query users by secondary index
List<User> admins = dynamoDb.objectsByIndex(
    "users",
    "username-index",
    new DynamoKey("username", "johndoe"),
    User.class
)

// Delete the user
dynamoDb.deleteItem("users", new DynamoKey("id", "user-123"))
```

### Working with Filters

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

// Find active users
def activeFilter = equals("active", true)
List<User> activeUsers = dynamoDb.objectsByIndex(
    "users",
    "username-index",
    new DynamoKey("username", "j"),
    User.class,
    activeFilter
)

// Find users with 'admin' in their roles
def adminFilter = contains("roles", "admin")
List<User> adminUsers = dynamoDb.objectsByIndex(
    "users",
    "email-index",
    new DynamoKey("email", "example.com"),
    User.class,
    adminFilter
)

// Complex filter combining conditions
def complexFilter = equals("active", true)
    .and(beginsWith("username", "j"))
    .and(contains("roles", "admin"))
    
List<User> filteredUsers = dynamoDb.objectsByIndex(
    "users",
    "email-index",
    new DynamoKey("email", "example.com"),
    User.class,
    complexFilter
)
```

### Handling Nested Objects

```groovy
class Address extends Dynable {
    String street
    String city
    String zipCode
    
    Address() {}
    
    Address(Map<String, Object> builder) {
        super(builder)
        this.street = builder.street
        this.city = builder.city
        this.zipCode = builder.zipCode
    }
    
    @Override
    protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
        return mapper
            .with('street', street)
            .with('city', city)
            .with('zipCode', zipCode)
    }
    
    @Override
    DynamoKey key() {
        // Not needed for nested objects, but must be implemented
        return null
    }
}

class Customer extends Dynable {
    String id
    String name
    Address address
    
    Customer() {}
    
    Customer(Map<String, Object> builder) {
        super(builder)
        this.id = builder.id
        this.name = builder.name
        
        if (builder.address) {
            this.address = new Address(builder.address as Map)
        }
    }
    
    @Override
    protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
        DynamoMapper addressMapper = new DynamoMapper()
        
        if (address) {
            addressMapper = address.impress(addressMapper, false) as DynamoMapper
        }
        
        return mapper
            .with('id', id, PARTITION_KEY)
            .with('name', name)
            .with('address', addressMapper)
    }
    
    @Override
    DynamoKey key() {
        return new DynamoKey('id', id)
    }
}

// Creating and storing a customer with address
def customer = new Customer(
    id: 'cust-001',
    name: 'ABC Corporation',
    address: new Address(
        street: '123 Main St',
        city: 'New York',
        zipCode: '10001'
    )
)

dynamoDb.putItem("customers", customer)
```

### Using Versioning

Impress supports optimistic locking with version tracking:

```groovy
// Enabling versioning (enabled by default)
dynamoDb.putItem("users", user, true)

// Disabling versioning
dynamoDb.putItem("users", user, false)
```

When versioning is enabled:
1. Objects store a version field (`v`)
2. On updates, the version is checked and incremented
3. Concurrent modifications will fail with a ConditionalCheckFailedException

## Advanced Usage

### Batch Operations

For adding multiple items at once:

```groovy
def users = [
    new User(id: 'user-1', username: 'user1', email: 'user1@example.com'),
    new User(id: 'user-2', username: 'user2', email: 'user2@example.com'),
    new User(id: 'user-3', username: 'user3', email: 'user3@example.com')
]

dynamoDb.putItems("users", users)
```

### Custom DynamoDB Client Configuration

```groovy
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

// Configure a custom DynamoDB client
def customClient = DynamoDbClient.builder()
    .region(Region.US_WEST_2)
    .credentialsProvider(ProfileCredentialsProvider.create("myprofile"))
    .build()
    
// Create DynamoDb handler with custom client
def dynamoDb = new DynamoDb(customClient)
```

### Local DynamoDB for Testing

For testing with DynamoDB Local:

```groovy
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import java.net.URI

// Connect to local DynamoDB instance
def localClient = DynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8000"))
    .region(Region.US_EAST_1)
    .credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create("dummy-key", "dummy-secret")
        )
    )
    .build()

def dynamoDb = new DynamoDb(localClient)
```

## Tips and Tricks

### Handling Null Values

When working with DynamoDB, null values require special handling:

```groovy
// In your Dynable implementation
@Override
protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
    mapper.with('id', id, PARTITION_KEY)
       .with('name', name)
       
    // Handle a nullable field by explicitly setting null
    if (description == null) {
        mapper.withNull('description')
    } else {
        mapper.with('description', description)
    }
    
    return mapper
}
```

### Working with Secondary Indexes

When querying with secondary indexes, remember to create them when setting up your table:

```groovy
// Create table with secondary indexes
dynamoDb.createTable(
    "products",
    "id",                     // Partition key
    "sku",                    // Sort key
    [
        "category-index": "category",
        "price-index": "price"
    ]
)

// Query using an index
List<Product> products = dynamoDb.objectsByIndex(
    "products",
    "category-index",
    new DynamoKey("category", "electronics"),
    Product.class
)
```

### Efficient Filtering

For efficient queries, remember:
1. Use partition keys for primary filtering
2. Use sort keys for range-based filtering
3. Use DynamoFilter for additional filtering (which happens client-side)

Example of optimal query structure:

```groovy
// Good: Using index for primary filtering, then applying additional filter
List<Order> orders = dynamoDb.objectsByIndex(
    "orders",
    "customer-index",
    new DynamoKey("customerId", "cust-123"),
    Order.class,
    greaterThan("orderTotal", 100)
)

// Less efficient: Filtering without appropriate index
List<Order> orders = dynamoDb.objectsByIndex(
    "orders",
    "status-index",
    new DynamoKey("status", "pending"),
    Order.class,
    equals("customerId", "cust-123").and(greaterThan("orderTotal", 100))
)
```

### Error Handling

```groovy
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

try {
    dynamoDb.putItem("users", user)
} catch (ConditionalCheckFailedException e) {
    // Handle version conflict
    log.warn("Concurrent modification detected: ${e.message}")
} catch (ResourceNotFoundException e) {
    // Handle missing table
    log.error("Table does not exist: ${e.message}")
} catch (Exception e) {
    // Handle other errors
    log.error("Error storing user: ${e.message}")
}
```

## License

[Add your license information here]