# Groovy Guide

This guide shows the idiomatic Groovy API. See the [Java guide](java.md) for
POJOs, records, and Java-specific mapper chaining.

## Connect

```groovy
import it.grational.storage.dynamodb.*

def dynamo = new DynamoDb()
```

To use an existing AWS SDK client:

```groovy
import software.amazon.awssdk.auth.credentials.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

def client = DynamoDbClient.builder()
  .region(Region.EU_WEST_1)
  .credentialsProvider(DefaultCredentialsProvider.create())
  .build()

def dynamo = new DynamoDb(client)
```

## Dynamic Items With DynamoMap

`DynamoMap` is the fastest way to work with flexible data. It implements
`Map<String, Object>` and can be saved directly.

```groovy
def user = new DynamoMap(
  id: 'user-1',
  name: 'Ada Lovelace',
  email: 'ada@example.com',
  profile: [
    department: 'Engineering',
    skills: ['Groovy', 'DynamoDB']
  ]
)

dynamo.putItem('users', user)

DynamoMap loaded = dynamo.getItem('users', KeyFilter.of('id', 'user-1'))
  .get()

assert loaded.name == 'Ada Lovelace'
assert loaded['email'] == 'ada@example.com'
```

If you want to insert a `DynamoMap` into a table before Impress can inspect the
table schema, specify the key fields:

```groovy
def item = new DynamoMap(
  [id: 'user-1', createdAt: 1700000000L, name: 'Ada'],
  'id',
  'createdAt'
)

dynamo.putItem('users', item)
```

## Domain Objects With Dynable

`Dynable` is a Groovy base class for versioned domain objects. It implements the
generic `Storable<AttributeValue, Object>` contract and automatically includes
the `v` version field when versioning is enabled.

```groovy
import it.grational.storage.DbMapper
import it.grational.storage.dynamodb.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static it.grational.storage.dynamodb.FieldType.*

class User extends Dynable {
  String id
  String email
  String name
  Integer loginCount = 0

  User() {}

  User(Map<String, Object> data) {
    super(data)
    id = data.id
    email = data.email
    name = data.name
    loginCount = (data.loginCount ?: 0) as Integer
  }

  @Override
  protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
    mapper
      .with('id', id, PARTITION_KEY)
      .with('email', email)
      .with('name', name)
      .with('loginCount', loginCount)
  }
}

def user = new User(id: 'user-1', email: 'ada@example.com', name: 'Ada')
dynamo.putItem('users', user)

User loaded = dynamo.getItem('users', KeyFilter.of('id', 'user-1'), User)
  .get()
```

## Query And Scan Builders

Builders make optional query parameters explicit and order-independent.

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

List<DynamoMap> active = dynamo.scan('users')
  .filter(match('status', 'active'))
  .fields('id', 'name', 'email')
  .take(50)
  .list()
```

Query by partition key:

```groovy
List<DynamoMap> orders = dynamo.query(
    'orders',
    KeyFilter.of('customerId', 'customer-1')
  )
  .backward()
  .take(10)
  .list()
```

Query by secondary index:

```groovy
List<DynamoMap> users = dynamo.query(
    'users',
    'email-index',
    KeyFilter.of('email', 'ada@example.com')
  )
  .list()
```

## Updating Nested Fields

Dot notation targets nested attributes:

```groovy
def update = new DynamoMapper()
  .with('id', 'user-1', PARTITION_KEY)
  .with('profile.settings.notifications', false)
  .with('profile.stats.visits', 11)

dynamo.updateItem('users', update)
```

Escape literal dots in attribute names:

```groovy
def update = new DynamoMapper()
  .with('id', 'user-1', PARTITION_KEY)
  .with('api\\.v2.endpoint', 'https://example.test')

dynamo.updateItem('users', update)
```
