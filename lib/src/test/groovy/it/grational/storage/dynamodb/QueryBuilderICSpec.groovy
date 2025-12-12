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
import it.grational.storage.dynamodb.DynamoFilter
import static it.grational.storage.dynamodb.DynamoFilter.*
// }}}

class QueryBuilderICSpec extends Specification {

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
		String sortKey
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

			if (sortKey)
				mapper.with('sortKey', sortKey, FieldType.SORT_KEY)

			if (data)
				mapper.with('data', data)

			if (tagField)
				mapper.with('tagField', tagField)

			mapper.with('enabled', enabled)

			if (versioned)
				mapper.with('version', version, FieldType.VERSION)

			return mapper
		}
	} // }}}

	def setupSpec() { // {{{
		dynamo = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)
	} // }}}

	def "Should limit the maximum number of items returned with take() method"() { // {{{
		given:
			String table = 'test_query_max'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and: 'Create more items than our max limit for single partition'
			List<TestItem> items = (1..15).collect { int i ->
				new TestItem(
					id: "user1",
					sortKey: String.format('%03d', i),
					data: "data${i}",
					enabled: true
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query with take() method to limit results'
			List<TestItem> takeResults = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.take(5)
				.list()

		then: 'Only maximum number of items should be returned'
			takeResults.size() == 5
			takeResults.every { it.id == 'user1' }
			takeResults.every { it.enabled == true }

		when: 'Use query without take() method'
			List<TestItem> allResults = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.list()

		then: 'All items for the partition should be returned'
			allResults.size() == 15

		when: 'Use take() with filter'
			List<TestItem> takeFilteredResults = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.filter(match('enabled', true))
				.take(3)
				.list()

		then: 'Only maximum number of filtered items should be returned'
			takeFilteredResults.size() == 3
			takeFilteredResults.every { it.id == 'user1' && it.enabled == true }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with take() and projection combined for query"() { // {{{
		given:
			String table = 'test_query_max_projection'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = (1..12).collect { int i ->
				new TestItem(
					id: "proj_user",
					sortKey: String.format('rec_%03d', i),
					data: "secret${i}",
					tagField: "tag${i}",
					enabled: true
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query with take() and projection'
			List<DynamoMap> projectedTakeResults = dynamo
				.query(table, KeyFilter.of('id', 'proj_user'))
				.fields(['id', 'sortKey', 'tagField'])
				.take(6)
				.list()

		then: 'Only projected fields and limited items should be returned'
			projectedTakeResults.size() == 6
			projectedTakeResults.every { item ->
				item.id == 'proj_user' &&
				item.sortKey != null &&
				item.tagField != null &&
				item.data == null &&
				item.enabled == null
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with take() and backward query"() { // {{{
		given:
			String table = 'test_query_max_backward'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = (1..10).collect { int i ->
				new TestItem(
					id: "back_user",
					sortKey: String.format('%03d', i),
					data: "data${i}"
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query with take() and backward'
			List<TestItem> backwardTakeResults = dynamo
				.query(table, KeyFilter.of('id', 'back_user'), TestItem)
				.backward()
				.take(4)
				.list()

		then: 'Only maximum items in reverse order should be returned'
			backwardTakeResults.size() == 4
			backwardTakeResults[0].sortKey == '010'
			backwardTakeResults[1].sortKey == '009'
			backwardTakeResults[2].sortKey == '008'
			backwardTakeResults[3].sortKey == '007'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with take() on index queries"() { // {{{
		given:
			String table = 'test_query_max_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'tag_index': 'tagField'
			]
		and:
			dynamo.createTable(table, partKey, null, indexes)
		and:
			List<TestItem> items = (1..18).collect { int i ->
				new TestItem(
					id: "item${i}",
					tagField: 'common_tag',
					data: "data${i}",
					enabled: i % 2 == 0
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query with index and take()'
			List<TestItem> indexTakeResults = dynamo
				.query(table, 'tag_index', KeyFilter.of('tagField', 'common_tag'), TestItem)
				.take(8)
				.list()

		then: 'Only maximum items from index should be returned'
			indexTakeResults.size() == 8
			indexTakeResults.every { it.tagField == 'common_tag' }

		when: 'Use query with index, filter and take()'
			List<TestItem> indexFilterTakeResults = dynamo
				.query(table, 'tag_index', KeyFilter.of('tagField', 'common_tag'), TestItem)
				.filter(match('enabled', true))
				.take(4)
				.list()

		then: 'Only maximum filtered items from index should be returned'
			indexFilterTakeResults.size() == 4
			indexFilterTakeResults.every { it.tagField == 'common_tag' && it.enabled == true }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle take() value of zero for query"() { // {{{
		given:
			String table = 'test_query_max_zero'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "zero_user", sortKey: "001", data: "data1"),
				new TestItem(id: "zero_user", sortKey: "002", data: "data2")
			]
			dynamo.putItems(table, items)

		when: 'Use query with take(0)'
			List<TestItem> zeroTakeResults = dynamo
				.query(table, KeyFilter.of('id', 'zero_user'), TestItem)
				.take(0)
				.list()

		then: 'No items should be returned'
			zeroTakeResults.size() == 0

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle take() value larger than available items for query"() { // {{{
		given:
			String table = 'test_query_max_larger'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "large_user", sortKey: "001", data: "data1"),
				new TestItem(id: "large_user", sortKey: "002", data: "data2"),
				new TestItem(id: "large_user", sortKey: "003", data: "data3")
			]
			dynamo.putItems(table, items)

		when: 'Use query with take() larger than available items'
			List<TestItem> largeTakeResults = dynamo
				.query(table, KeyFilter.of('id', 'large_user'), TestItem)
				.take(100)
				.list()

		then: 'All available items should be returned'
			largeTakeResults.size() == 3

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should use DynamoMap as default class when none specified"() { // {{{
		given:
			String table = 'test_query_default_class'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "default_user", sortKey: "001", data: "test_data"),
				new TestItem(id: "default_user", sortKey: "002", data: "more_data")
			]
			dynamo.putItems(table, items)

		when: 'Use query without specifying class'
			List<DynamoMap> defaultResults = dynamo
				.query(table, KeyFilter.of('id', 'default_user'))
				.list()

		then: 'Should return DynamoMap instances'
			defaultResults.size() == 2
			defaultResults.every { it instanceof DynamoMap }
			defaultResults.every { it.id == 'default_user' }
			defaultResults.any { it.data == 'test_data' }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should allow specifying class using as() method"() { // {{{
		given:
			String table = 'test_query_as_method'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "as_user", sortKey: "001", data: "typed_data"),
				new TestItem(id: "as_user", sortKey: "002", data: "more_typed_data")
			]
			dynamo.putItems(table, items)

		when: 'Use query and specify class with as() method'
			List<TestItem> typedResults = dynamo
				.query(table, KeyFilter.of('id', 'as_user'))
				.as(TestItem)
				.list()

		then: 'Should return TestItem instances'
			typedResults.size() == 2
			typedResults.every { it instanceof TestItem }
			typedResults.every { it.id == 'as_user' }
			typedResults.any { it.data == 'typed_data' }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

}
// vim: fdm=marker
