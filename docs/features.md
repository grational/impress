# Feature Guide

This page summarizes the main features exposed by Impress. Examples are in
Groovy unless Java syntax materially differs; see the [Java guide](java.md) for
Java-specific examples.

## Table Creation

Simple table:

```groovy
dynamo.createTable('users', 'id')
```

Composite key:

```groovy
dynamo.createTable('orders', 'customerId', 'createdAt')
```

Secondary indexes:

```groovy
dynamo.createTable(
  'users',
  'id',
  [
    Index.of('email'),
    Index.of('status', 'createdAt', 'status-createdAt-index')
  ] as Index[]
)
```

Typed keys and indexes:

```groovy
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

dynamo.createTable(
  'metrics',
  Scalar.of('metricId', ScalarAttributeType.S),
  Optional.of(Scalar.of('timestamp', ScalarAttributeType.N)),
  [
    Index.of(
      Scalar.of('category', ScalarAttributeType.S),
      Scalar.of('value', ScalarAttributeType.N),
      'category-value-index'
    )
  ] as Index[]
)
```

Billing options:

```groovy
// Default: PAY_PER_REQUEST
dynamo.createTable('users', 'id')

// Explicit provisioned throughput for table and indexes
dynamo.createTable(
  'users',
  'id',
  [Index.of('email')] as Index[],
  BillingOptions.provisioned(5, 5)
)

// Separate table and index throughput
dynamo.createTable(
  'orders',
  'customerId',
  'createdAt',
  [Index.of('status', 'createdAt')] as Index[],
  BillingOptions.provisioned(10, 5, 3, 2)
)
```

## Put And Batch Write

```groovy
dynamo.putItem('users', new DynamoMap(
  id: 'user-1',
  name: 'Ada'
))
```

Batch writes are split into DynamoDB transaction-sized chunks:

```groovy
dynamo.putItems('users', users)
```

## GetItem Builder

```groovy
DynamoMap user = dynamo.getItem('users', KeyFilter.of('id', 'user-1'))
  .fields('id', 'name', 'email')
  .get()
```

Typed result:

```groovy
User user = dynamo.getItem('users', KeyFilter.of('id', 'user-1'), User)
  .get()
```

Java callers can use `.returning(User.class)`.

## Query Builder

```groovy
List<DynamoMap> orders = dynamo.query(
    'orders',
    KeyFilter.of('customerId', 'customer-1')
  )
  .backward()
  .take(20)
  .list()
```

Query an index:

```groovy
List<DynamoMap> users = dynamo.query(
    'users',
    'status-index',
    KeyFilter.of('status', 'active')
  )
  .list()
```

Paged query:

```groovy
PagedResult<DynamoMap> page = dynamo.query(
    'orders',
    KeyFilter.of('customerId', 'customer-1')
  )
  .paged(50)
```

## Scan Builder

```groovy
import static it.grational.storage.dynamodb.DynamoFilter.*

List<DynamoMap> active = dynamo.scan('users')
  .filter(match('status', 'active'))
  .fields('id', 'name')
  .take(100)
  .list()
```

Parallel scan segment:

```groovy
List<DynamoMap> segment = dynamo.scan('users')
  .segment(0, 4)
  .limit(100)
  .list()
```

## `take()` Versus `limit()`

`take(n)` controls the total number of returned items across all pages.

`limit(n)` controls DynamoDB page size. It is useful when you want manual
pagination or predictable request sizes.

```groovy
dynamo.scan('users')
  .filter(match('status', 'active'))
  .take(25)
  .list()
```

## Filtering

Basic filters:

```groovy
match('status', 'active')
greater('score', 90)
less('createdAt', cutoff)
between('price', 10, 100)
contains('tags', 'premium')
beginsWith('email', 'ada')
defined('email')
```

Composed filters:

```groovy
every(
  match('department', 'Engineering'),
  any(
    greater('score', 90),
    contains('skills', 'DynamoDB')
  ),
  defined('email')
)
```

Nested fields use dot notation:

```groovy
match('profile.department', 'Engineering')
greater('subscription.usage.apiCalls', 1000)
```

Literal dots can be escaped:

```groovy
match('api\\.v2.status', 'ok')
```

## Key Conditions

Exact partition key:

```groovy
KeyFilter.of('userId', 'user-1')
```

Exact composite key:

```groovy
KeyFilter.of('userId', 'user-1', 'createdAt', 1700000000L)
```

Sort-key range:

```groovy
KeyFilter recent = KeyFilter
  .partition('userId', 'user-1')
  .sort(greater('createdAt', 1700000000L))
  .build()
```

## Projection

Projection reduces the fields returned by DynamoDB:

```groovy
dynamo.getItem('users', KeyFilter.of('id', 'user-1'))
  .fields('id', 'name', 'profile.department')
  .get()
```

Projection works on get, query, and scan builders.

## Updates

Save an item:

```groovy
dynamo.putItem('users', user)
```

Update selected fields:

```groovy
def update = new DynamoMapper()
  .with('id', 'user-1')
  .with('name', 'Ada')
  .with('profile.settings.notifications', false)

dynamo.updateItem('users', update)
```

Remove fields:

```groovy
def update = new DynamoMapper()
  .with('id', 'user-1')
  .remove('obsoleteField', 'profile.oldSetting')

dynamo.updateItem('users', update)
```

## Automatic Key Extraction

For update, delete, and refresh workflows, Impress can inspect the table schema
and extract key attributes from a storable item:

```groovy
def user = new DynamoMap(id: 'user-1', name: 'Ada')

dynamo.updateItem('users', user.tap { name = 'Ada Lovelace' })
dynamo.refreshItem('users', user)
dynamo.deleteItem('users', user)
```

The same applies to a `DynamoMapper` passed to `updateItem`:

```groovy
def update = new DynamoMapper()
  .with('id', 'user-1')
  .with('name', 'Ada Lovelace')

dynamo.updateItem('users', update)
```

Use explicit `FieldType.PARTITION_KEY` and `FieldType.SORT_KEY` only when you
need a standalone mapper to expose `mapper.key()` before a `DynamoDb` operation
has a table name available.

## Optimistic Locking

`Dynable` uses the `v` field as a version field when versioning is enabled.

```groovy
dynamo.putItem('users', user, true)
```

On subsequent saves, the version condition prevents overwriting a newer item.

## Pagination

Automatic pagination:

```groovy
List<DynamoMap> all = dynamo.query('orders', KeyFilter.of('customerId', 'c1'))
  .list()
```

Manual pagination:

```groovy
Map<String, AttributeValue> lastKey = null

do {
  PagedResult<DynamoMap> page = dynamo.scan('orders')
    .paged(100, lastKey)

  process(page.items)
  lastKey = page.last
} while (lastKey)
```
