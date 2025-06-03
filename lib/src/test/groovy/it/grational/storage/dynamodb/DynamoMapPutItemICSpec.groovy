package it.grational.storage.dynamodb

import spock.lang.Specification
import spock.lang.Shared
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import static it.grational.storage.dynamodb.FieldType.*

class DynamoMapPutItemTestSpec extends Specification {

	@Shared
	DynamoDb dynamo
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()

	def setupSpec() { // {{{
		dynamo = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)
	} // }}}

	def "Should fail if the DynamoMap has no key configuration"() { // {{{
		given: "A table with partition key"
			String table = 'test_fail'
			dynamo.createTable(table, 'id')
		and: "A DynamoMap without key configuration"
			DynamoMap item = new DynamoMap (
				id: 'test1',
				data: 'value'
			)

		when: "Trying to use putItem"
			dynamo.putItem(table, item)
		and: "Retrieving with a manually created key"
			DynamoMap retrieved = dynamo.getItem (
				table,
				KeyFilter.of('id', 'test1')
			)

		then: "Item is saved but key retrieval shows the issue"
			retrieved != null
			retrieved.id == 'test1'
		and: "The DynamoMap doesn't generate proper keys"
			DynamoMapper mapper = item.impress(new DynamoMapper())
			!mapper.hasKey()

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should test DynamoMap compatibility with putItem methods"() { // {{{
		given:
			String table = 'test_dynamomap_putitem'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and: "Create a DynamoMap with key information"
			DynamoMap dynamoMap = new DynamoMap (
				id: 'test123',
				name: 'Test Item',
				value: 42
			).withPartitionKey('id')

		when: "Call putItem with DynamoMap"
			dynamo.putItem(table, dynamoMap)

		then: "Should succeed"
			notThrown(Exception)

		when: "Retrieve the item"
			KeyFilter key = new KeyFilter(
				dynamoMap.impress(new DynamoMapper()).key()
			)
			DynamoMap retrieved = dynamo.getItem (
				table,
				key
			)

		then: "Should find the item"
			retrieved != null
			retrieved.id == 'test123'
			retrieved.name == 'Test Item'
			retrieved.value == 42

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should verify impress method now sets keys when configured"() { // {{{
		given:
			DynamoMap dynamoMap = new DynamoMap(
				id: 'test123',
				sortKey: 'sort001',
				name: 'Test Item'
			).withKeys('id', 'sortKey')

		when:
			DynamoMapper mapper = dynamoMap.impress (
				new DynamoMapper(), true
			) as DynamoMapper

		then:
			mapper.hasKey()
			!mapper.key().isEmpty()
			mapper.key().size() == 2
	} // }}}

	def "Should support DynamoMap with composite key in putItems"() { // {{{
		given:
			String table = 'test_dynamomap_composite'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and: "Create multiple DynamoMaps with composite keys"
			List<DynamoMap> items = [
				new DynamoMap ( // alternative method to set key names
					[ id: 'parent1',
						sortKey: 'child1',
						name: 'Item 1'
					],
					'id', 'sortKey'
				),
				new DynamoMap (
					id: 'parent1',
					sortKey: 'child2',
					name: 'Item 2'
				).withKeys('id', 'sortKey')
			]

		when: "Call putItems with DynamoMaps"
			dynamo.putItems(table, items)

		then: "Should succeed"
			notThrown(Exception)

		when: "Retrieve the items"
			KeyFilter key1 = new KeyFilter (
				items[0].impress(new DynamoMapper()
			).key())
			DynamoMap retrieved1 = dynamo.getItem(table, key1, DynamoMap)

		then: "Should find the first item"
			retrieved1 != null
			retrieved1.id == 'parent1'
			retrieved1.sortKey == 'child1'
			retrieved1.name == 'Item 1'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

}

// vim: fdm=marker
