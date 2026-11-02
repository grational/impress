package it.grational.storage.dynamodb

import spock.lang.Specification
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

class TransactionUSpec extends Specification {
	static class Item extends Dynable {
		String id
		int amount
		Item(Map data) { super(data); id = data.id; amount = data.amount }
		protected it.grational.storage.DbMapper inpress(DynamoMapper mapper) {
			mapper.with('id', id).with('amount', amount)
		}
	}
	def client = Mock(DynamoDbClient)
	def dynamo = new DynamoDb(client)

	def 'one frozen request spans tables and retries without incrementing versions again'() {
		given:
		def item = new Item([id: 'one', v: 3, amount: 10])
		def tx = dynamo.transaction('operation').put('budgets', [item]).put('journal', [new DynamoMap([id: 'entry'])])
		def requests = []

		when:
		tx.commit()
		item.amount = 99
		tx.commit()

		then:
		2 * client.transactWriteItems(_ as TransactWriteItemsRequest) >> { TransactWriteItemsRequest request ->
			requests << request
			TransactWriteItemsResponse.builder().build()
		}
		requests[0].is(requests[1])
		requests[0].clientRequestToken() == 'operation'
		requests[0].transactItems()*.put()*.tableName() == ['budgets', 'journal']
		requests[0].transactItems()[0].put().item().v.n() == '4'
		requests[1].transactItems()[0].put().item().amount.n() == '10'

		when:
		tx.put('budgets', item)
		then:
		thrown(IllegalStateException)
	}

	def 'oversized transaction fails without sending any fragment'() {
		when:
		dynamo.transaction('large').put('budgets', (1..101).collect { new DynamoMap([id: "$it"]) }).commit()
		then:
		thrown(IllegalArgumentException)
		0 * client.transactWriteItems(_ as TransactWriteItemsRequest)
	}

	def 'consistent reads are opt in'() {
		when:
		dynamo.getItem('budgets', KeyFilter.of('id', 'one')).consistent().get()
		dynamo.getItem('budgets', KeyFilter.of('id', 'one')).get()
		then:
		1 * client.getItem({ it.consistentRead() == true } as GetItemRequest) >> GetItemResponse.builder().build()
		1 * client.getItem({ !it.consistentRead() } as GetItemRequest) >> GetItemResponse.builder().build()
	}

	def 'delete and attribute removal carry their conditions in the same transaction'() {
		when:
		dynamo.transaction('repair')
			.delete('journal', KeyFilter.of('id', 'entry'), DynamoFilter.match('v', 2))
			.update('products', KeyFilter.of('id', 'product'), new DynamoMapper().remove('activationStatus'), DynamoFilter.match('activation', '2026-11-02'))
			.commit()
		then:
		1 * client.transactWriteItems({ request ->
			request.transactItems()[0].delete().conditionExpression() &&
			request.transactItems()[1].update().updateExpression().startsWith('REMOVE') &&
			request.transactItems()[1].update().conditionExpression()
		} as TransactWriteItemsRequest) >> TransactWriteItemsResponse.builder().build()
	}
}
