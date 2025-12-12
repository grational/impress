package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import it.grational.storage.dynamodb.DynamoDb
import it.grational.storage.dynamodb.DynamoMapper
import it.grational.storage.dynamodb.DynamoMap
import it.grational.storage.dynamodb.KeyFilter
import it.grational.storage.dynamodb.FieldType

class DynamoNestedUpdateICSpec extends Specification {

	@Shared
	DynamoDb dynamo
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()

	def setupSpec() {
		dynamo = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)
	}

	def "Should update nested field without replacing the entire object"() {
		given:
			String table = 'test_nested_update'
			String partKey = 'id'
			dynamo.createTable(table, partKey)

		and:
			// Initial item with a map
			DynamoMap item = new DynamoMap([
				id: 'item1',
				info: [
					name: 'John',
					age: 30,
					details: [
						city: 'New York',
						active: true
					]
				]
			])
			dynamo.putItem(table, item)

		when: 'We verify the initial state'
			DynamoMap stored = dynamo.getItem(
				table,
				KeyFilter.of('id', 'item1'),
				DynamoMap
			).get()

		then:
			stored.info.name == 'John'
			stored.info.age == 30
			stored.info.details.city == 'New York'

		when: 'We try to update nested fields using dot notation'
			DynamoMapper updateMapper = new DynamoMapper()
				.with('id', 'item1', FieldType.PARTITION_KEY)
				.with('info.age', 31)
				.with('info.details.city', 'Boston')

			dynamo.updateItem(table, updateMapper)

		then: 'The nested fields should be updated and other fields preserved'
			DynamoMap updated = dynamo.getItem(
				table,
				KeyFilter.of('id', 'item1'),
				DynamoMap
			).get()

			// Check updated fields
			updated.info.age == 31
			updated.info.details.city == 'Boston'

			// Check preserved fields
			updated.info.name == 'John'
			updated.info.details.active == true
			
			// Verify no weird top-level attributes were created
			!updated.containsKey('info.age')
			!updated.containsKey('info.details.city')

		cleanup:
			dynamo.dropTable(table)
	}
}
