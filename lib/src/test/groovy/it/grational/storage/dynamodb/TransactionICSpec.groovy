package it.grational.storage.dynamodb

import spock.lang.Specification
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

class TransactionICSpec extends Specification {
	def client = DynamoDbClient.builder().endpointOverride(URI.create('http://localhost:8888')).build()
	def db = new DynamoDb(client)
	String table = 'atomic_' + UUID.randomUUID().toString().replace('-', '')

	def setup() { db.createTable(table, 'id') }
	def cleanup() { db.dropTable(table); client.close() }

	def '4 MB rejection is indivisible and 100 small items fit'() {
		when:
		db.transaction().put(table, (1..15).collect { new DynamoMap([id: "$it", data: 'x' * 300000]) }).commit()
		then:
		thrown(DynamoDbException)
		db.scan(table).list().empty
		when:
		db.transaction().put(table, (1..100).collect { new DynamoMap([id: "$it"]) }).commit()
		then:
		db.scan(table).list().size() == 100
	}

	def 'conditional delete and restore do not overwrite a changed or occupied key'() {
		given:
		def item = new DynamoMap([id: 'entry', amount: 3])
		db.putItem(table, item)
		when:
		db.transaction().delete(table, KeyFilter.of('id', 'entry'), DynamoFilter.match('amount', 2)).commit()
		then:
		thrown(TransactionCanceledException)
		db.getItem(table, KeyFilter.of('id', 'entry')).consistent().get().amount == 3
		when:
		db.transaction().delete(table, KeyFilter.of('id', 'entry'), DynamoFilter.match('amount', 3)).commit()
		db.transaction().put(table, item, new DynamoFilter('attribute_not_exists(#id)', ['#id': 'id'], [:])).commit()
		then:
		db.getItem(table, KeyFilter.of('id', 'entry')).get().amount == 3
		when:
		db.transaction().put(table, new DynamoMap([id: 'entry', amount: 9]), new DynamoFilter('attribute_not_exists(#id)', ['#id': 'id'], [:])).commit()
		then:
		thrown(TransactionCanceledException)
		db.getItem(table, KeyFilter.of('id', 'entry')).get().amount == 3
	}

	def 'conditional removal preserves unrelated fields and rejects a different activation date'() {
		given:
		db.putItem(table, new DynamoMap([id: 'product', activation: '2026-11-02', activationStatus: 'PENDING', other: 'preserved']))
		when:
		db.transaction().update(table, KeyFilter.of('id', 'product'), new DynamoMapper().remove('activationStatus'), DynamoFilter.match('activation', '2026-11-03')).commit()
		then:
		thrown(TransactionCanceledException)
		when:
		db.transaction().update(table, KeyFilter.of('id', 'product'), new DynamoMapper().remove('activationStatus'), DynamoFilter.match('activation', '2026-11-02')).commit()
		then:
		def product = db.getItem(table, KeyFilter.of('id', 'product')).get()
		!product.containsKey('activationStatus')
		product.other == 'preserved'
	}
}
