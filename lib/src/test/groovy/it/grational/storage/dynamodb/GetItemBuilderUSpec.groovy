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
import it.grational.storage.dynamodb.DynamoMap
import it.grational.storage.dynamodb.KeyFilter
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

	@Unroll
	def "Should support nested field projection with field #nestedField"() { // {{{
		given:
			String table = 'test-nested-projection'
			String partKey = 'id'
			dynamo.createTable(table, partKey)
			
			// Sample item with nested structure
			DynamoMap item = new DynamoMap (
				id: 'nested-test',
				address: [
					street: '123 Main St',
					city: 'Springfield',
					coordinates: [lat: 42.1, lng: -71.2]
				],
				metadata: [
					tags: [ 'important', 'customer', 'premium' ],
					created: '2023-01-01'
				],
				users: [
					[ name: 'John', role: 'admin'  ],
					[ name: 'Jane', role: 'user'   ],
					[ name: 'Bob',  role: 'viewer' ]
				],
				config: [
					settings: [
						timeout: 30,
						retries: 3
					]
				]
			)
			
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'nested-test'))
				.fields('id', nestedField)
				.get()

		then:
			result != null
			result.id == 'nested-test'
			
			// Verify only the projected nested field is present
			switch (nestedField) {
				case 'address.street':
					assert result.address?.street == '123 Main St'
					assert result.address?.city == null  // Not projected
					assert result.metadata == null       // Not projected
					break
				case 'metadata.tags[0]':
					assert result.metadata?.tags?[0] == 'important'
					assert result.metadata?.created == null  // Not projected
					assert result.address == null            // Not projected
					break
				case 'users[1].name':
					assert result.users?[0]?.name == 'Jane'
					assert result.users?[0]?.role == null  // Not projected
					assert result.address == null          // Not projected
					break
				case 'config.settings.timeout':
					assert result.config?.settings?.timeout == 30
					assert result.config?.settings?.retries == null  // Not projected
					assert result.address == null                     // Not projected
					break
			}

		cleanup:
			dynamo.dropTable(table)

		where:
			nestedField << [
				'address.street',
				'metadata.tags[0]',
				'users[1].name',
				'config.settings.timeout'
			]
	} // }}}

	def "Should support multiple nested field projections"() { // {{{
		given:
			String table = 'test-multiple-nested'
			String partKey = 'id'
			dynamo.createTable(table, partKey)
			
			DynamoMap item = new DynamoMap([
				id: 'multi-nested-test',
				profile: [
					name: 'John Doe',
					contact: [
						email: 'john@example.com',
						phone: '555-1234'
					]
				],
				preferences: [
					theme: 'dark',
					notifications: [
						email: true,
						sms: false
					]
				],
				history: [
					[action: 'login', timestamp: '2023-01-01'],
					[action: 'logout', timestamp: '2023-01-02']
				]
			])
			
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'multi-nested-test'))
				.fields('id', 'profile.name', 'preferences.notifications.email', 'history[0].action')
				.get()

		then:
			result != null
			result.id == 'multi-nested-test'
			result.profile?.name == 'John Doe'
			result.profile?.contact == null  // Not projected
			result.preferences?.notifications?.email == true
			result.preferences?.theme == null  // Not projected
			result.history?[0]?.action == 'login'
			result.history?[0]?.timestamp == null  // Not projected

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "should handle nested projection with reserved keywords"() { // {{{
		given:
			String table = 'test-nested-reserved'
			String partKey = 'id'
			dynamo.createTable(table, partKey)
			
			DynamoMap item = new DynamoMap([
				id: 'reserved-nested-test',
				data: [
					'size': 'large',     // 'size' is a DynamoDB reserved keyword
					'count': 5           // 'count' is a DynamoDB reserved keyword
				],
				status: 'active'         // 'status' is a DynamoDB reserved keyword
			])
			
			dynamo.putItem(table, item)

		when:
			DynamoMap result = dynamo
				.getItem(table, KeyFilter.of('id', 'reserved-nested-test'))
				.fields('id', 'data.size', 'status')
				.get()

		then:
			result != null
			result.id == 'reserved-nested-test'
			result.data?.size == 'large'
			result.data?.count == null  // Not projected
			result.status == 'active'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

}
// vim: fdm=marker
