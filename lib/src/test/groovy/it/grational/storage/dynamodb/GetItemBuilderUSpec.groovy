package it.grational.storage.dynamodb

// imports {{{
import spock.lang.*
import groovy.transform.ToString
import groovy.transform.EqualsAndHashCode
// dynamodb
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// local
import it.grational.storage.Storable
import it.grational.storage.DbMapper
// }}}

class GetItemBuilderUSpec extends Specification {

	// fields {{{
	@Shared
	DynamoDb dynamo
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()
	// }}}

	@ToString(includeNames = true)
	@EqualsAndHashCode
	static class TestItem
		implements Storable<AttributeValue, Object> { // {{{
		String id
		String data
		String tagField
		boolean enabled = true
		Integer version = 1

		@Override
		DbMapper<AttributeValue, Object> impress (
			DbMapper<AttributeValue, Object> mapper,
			boolean versioned = true
		) {
			mapper.with (
				'id', id,
				FieldType.PARTITION_KEY
			)

			if (data)
				mapper.with('data', data)

			if (tagField)
				mapper.with('tagField', tagField)

			mapper.with('enabled', enabled)

			if (version)
				mapper.with (
					'version', version,
					versioned
						? FieldType.VERSION
						: FieldType.STANDARD
				)
			return mapper
		}

		TestItem(Map<String, Object> source) {
			this.id = source.id
			this.data = source.data
			this.tagField = source.tagField
			this.enabled = source.containsKey('enabled') ? source.enabled : true
			this.version = source.version ?: 1
		}
	} // }}}

	def setupSpec() { // {{{
		dynamo = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)
	} // }}}

	def "Should get item using builder pattern without projection"() { // {{{
		given:
			String table = 'test_getitem_basic'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			TestItem item = new TestItem (
				id: 'item1',
				data: 'test data',
				tagField: 'category_a',
				enabled: true
			)
			dynamo.putItem(table, item)

		when:
			TestItem result = dynamo
				.getItem(table, KeyFilter.of('id', 'item1'), TestItem)
				.get()

		then:
			result.id == 'item1'
			result.data == 'test data'
			result.tagField == 'category_a'
			result.enabled == true
			result.version == 2

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should get item using builder pattern with field projection"() { // {{{
		given:
			String table = 'test_getitem_projection'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			TestItem item = new TestItem (
				id: 'item2',
				data: 'sensitive data',
				tagField: 'category_b',
				enabled: false
			)
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'item2'))
				.fields('id', 'tagField')
				.get()

		then:
			result.id == 'item2'
			result.tagField == 'category_b'
			result.data == null
			result.enabled == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should get item using builder pattern with varargs field projection"() { // {{{
		given:
			String table = 'test_getitem_varargs'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			TestItem item = new TestItem (
				id: 'item3',
				data: 'more data',
				tagField: 'category_c',
				enabled: true
			)
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'item3'))
				.fields('id', 'data', 'enabled')
				.get()

		then:
			result.id == 'item3'
			result.data == 'more data'
			result.enabled == true
			result.tagField == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should return null when item does not exist"() { // {{{
		given:
			String table = 'test_getitem_null'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)

		when:
			TestItem result = dynamo
				.getItem(table, KeyFilter.of('id', 'nonexistent'), TestItem)
				.get()

		then:
			result == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with composite keys"() { // {{{
		given:
			String table = 'test_getitem_composite'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			DynamoMap item = new DynamoMap([
				id: 'user1',
				sortKey: '2023',
				data: 'composite data'
			])
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'user1', 'sortKey', '2023'))
				.get()

		then:
			result.id == 'user1'
			result.sortKey == '2023'
			result.data == 'composite data'

		when: 'Try to get with wrong sort key'
			DynamoMap notFound = dynamo
				.getItem(table, KeyFilter.of('id', 'user1', 'sortKey', '2024'))
				.get()

		then:
			notFound == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle projection with reserved keywords"() { // {{{
		given:
			String table = 'test_getitem_reserved'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			DynamoMap item = new DynamoMap([
				id: 'reserved1',
				name: 'test item',
				status: 'active',
				type: 'document',
				size: 1024
			])
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'reserved1'))
				.fields('id', 'name', 'status', 'type', 'size')
				.get()

		then:
			result.id == 'reserved1'
			result.name == 'test item'
			result.status == 'active'
			result.type == 'document'
			result.size == 1024

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with different return types"() { // {{{
		given:
			String table = 'test_getitem_types'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			TestItem item = new TestItem (
				id: 'types1',
				data: 'typed data',
				tagField: 'type_test',
				enabled: false
			)
			dynamo.putItem(table, item, false)

		when: 'Get as TestItem'
			TestItem typedResult = dynamo
				.getItem(table, KeyFilter.of('id', 'types1'), TestItem)
				.get()

		then:
			typedResult instanceof TestItem
			typedResult.id == 'types1'
			typedResult.data == 'typed data'
			typedResult.enabled == false
			typedResult.version == 1

		when: 'Get as DynamoMap (default)'
			DynamoMap mapResult = dynamo
				.getItem(table, KeyFilter.of('id', 'types1'))
				.get()

		then:
			mapResult instanceof DynamoMap
			mapResult.id == 'types1'
			mapResult.data == 'typed data'
			mapResult.enabled == false
			mapResult.version == 1

		cleanup:
			dynamo.dropTable(table)
	} // }}}

}
// vim: fdm=marker
