package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

/** One indivisible write, frozen on first submission, including its idempotency token. */
@CompileStatic
class Transaction {
	private final DynamoDbClient client
	private final String token
	private final List<Closure<TransactWriteItem>> actions = []
	private TransactWriteItemsRequest request
	private boolean frozen = false
	private long firstSubmission

	Transaction(DynamoDbClient client, String token) {
		if (!token || token.length() > 36)
			throw new IllegalArgumentException('Transaction token must contain 1 to 36 characters')
		this.client = client
		this.token = token
	}

	Transaction put(String table, Storable<AttributeValue, Object> item, boolean versioned = true) {
		add { TransactionItems.put(table, item, versioned) }
	}

	Transaction put(String table, Iterable<? extends Storable<AttributeValue, Object>> items, boolean versioned = true) {
		items.each { Storable<AttributeValue, Object> item -> put(table, item, versioned) }
		return this
	}

	Transaction put(String table, Storable<AttributeValue, Object> item, DynamoFilter condition) {
		add { TransactionItems.put(table, item, condition) }
	}

	Transaction delete(String table, KeyFilter key, DynamoFilter condition) {
		add { TransactionItems.delete(table, key, condition) }
	}

	Transaction update(String table, KeyFilter key, DynamoMapper changes, DynamoFilter condition) {
		add { TransactionItems.update(table, key, changes, condition) }
	}

	private Transaction add(Closure<TransactWriteItem> action) {
		if (frozen)
			throw new IllegalStateException('A submitted transaction cannot be changed')
		actions.add(action)
		return this
	}

	synchronized void commit() {
		if (!frozen) {
			if (actions.size() > 100)
				throw new IllegalArgumentException('A transaction cannot exceed 100 actions')
			request = TransactWriteItemsRequest.builder().clientRequestToken(token)
				.transactItems(actions.collect { Closure<TransactWriteItem> action -> action.call() }).build()
			frozen = true
			firstSubmission = System.nanoTime()
		}
		if (!actions)
			return
		// Stop before DynamoDB's ten-minute token expiry, even after an uncertain response.
		if (System.nanoTime() - firstSubmission >= 540_000_000_000L)
			throw new IllegalStateException('Transaction replay window expired; outcome must be reconciled')
		// DynamoDB validates the aggregate 4 MB limit atomically; never split this request.
		client.transactWriteItems(request)
	}
}
