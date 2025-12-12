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

class ScanBuilderICSpec extends Specification {

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
			String table = 'test_scan_max'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and: 'Create more items than our max limit'
			List<TestItem> items = (1..20).collect { int i ->
				new TestItem(
					id: "item${i}",
					data: "data${i}",
					enabled: true
				)
			}
			dynamo.putItems(table, items)

		when: 'Use scan with take() method to limit results'
			List<TestItem> takeResults = dynamo
				.scan(table, TestItem)
				.take(5)
				.list()

		then: 'Only maximum number of items should be returned'
			takeResults.size() == 5
			takeResults.every { it.enabled == true }

		when: 'Use scan without take() method'
			List<TestItem> allResults = dynamo
				.scan(table, TestItem)
				.list()

		then: 'All items should be returned'
			allResults.size() == 20

		when: 'Use take() with filter'
			List<TestItem> takeFilteredResults = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.take(3)
				.list()

		then: 'Only maximum number of filtered items should be returned'
			takeFilteredResults.size() == 3
			takeFilteredResults.every { it.enabled == true }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with take() and projection combined"() { // {{{
		given:
			String table = 'test_scan_max_projection'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = (1..15).collect { int i ->
				new TestItem(
					id: "proj${i}",
					data: "secret${i}",
					tagField: "tag${i}",
					enabled: true
				)
			}
			dynamo.putItems(table, items)

		when: 'Use scan with take() and projection'
			List<DynamoMap> projectedTakeResults = dynamo
				.scan(table)
				.fields(['id', 'tagField'])
				.take(7)
				.list()

		then: 'Only projected fields and limited items should be returned'
			projectedTakeResults.size() == 7
			projectedTakeResults.every { item ->
				item.id != null &&
				item.tagField != null &&
				item.data == null &&
				item.enabled == null
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should work with take() and segment scanning"() { // {{{
		given:
			String table = 'test_scan_max_segment'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = (1..25).collect { int i ->
				new TestItem(
					id: "seg${i}",
					data: "data${i}",
					enabled: i % 2 == 0
				)
			}
			dynamo.putItems(table, items)

		when: 'Use scan with take() and segment'
			List<TestItem> segmentTakeResults = dynamo
				.scan(table, TestItem)
				.segment(0, 2)
				.take(4)
				.list()

		then: 'Only maximum items from the segment should be returned'
			segmentTakeResults.size() <= 4

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle take() value of zero"() { // {{{
		given:
			String table = 'test_scan_max_zero'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "zero1", data: "data1"),
				new TestItem(id: "zero2", data: "data2")
			]
			dynamo.putItems(table, items)

		when: 'Use scan with take(0)'
			List<TestItem> zeroTakeResults = dynamo
				.scan(table, TestItem)
				.take(0)
				.list()

		then: 'No items should be returned'
			zeroTakeResults.size() == 0

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle take() value larger than available items"() { // {{{
		given:
			String table = 'test_scan_max_larger'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "large1", data: "data1"),
				new TestItem(id: "large2", data: "data2"),
				new TestItem(id: "large3", data: "data3")
			]
			dynamo.putItems(table, items)

		when: 'Use scan with take() larger than available items'
			List<TestItem> largeTakeResults = dynamo
				.scan(table, TestItem)
				.take(100)
				.list()

		then: 'All available items should be returned'
			largeTakeResults.size() == 3

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should continue scanning when pages are empty due to filtering (sparse data)"() { // {{{
		given:
			String table = 'test_scan_sparse'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and: 'Create a haystack with needles at the end'
			List<TestItem> items = []
			// Add 20 "noise" items that won't match the filter
			(1..20).each { items << new TestItem(id: "noise_${it}", data: "noise", enabled: false) }
			// Add 2 "valid" items at the end
			items << new TestItem(id: "valid_1", data: "precious", enabled: true)
			items << new TestItem(id: "valid_2", data: "precious", enabled: true)
			
			dynamo.putItems(table, items)

		when: 'Use scan with take(2) and a filter that rejects most items'
			// Logic: take(2) implies Dynamo Limit=2 per request. 
			// Since the first 20 items don't match, Dynamo will return many empty pages 
			// with a LastEvaluatedKey before finding the valid ones.
			List<TestItem> result = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.take(2)
				.list()

		then: 'It should persevere through empty pages and find the items'
			result.size() == 2
			result.every { it.enabled == true }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should use DynamoMap as default class when none specified"() { // {{{
		given:
			String table = 'test_scan_default_class'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "default1", data: "test_data"),
				new TestItem(id: "default2", data: "more_data")
			]
			dynamo.putItems(table, items)

		when: 'Use scan without specifying class'
			List<DynamoMap> defaultResults = dynamo
				.scan(table)
				.list()

		then: 'Should return DynamoMap instances'
			defaultResults.size() == 2
			defaultResults.every { it instanceof DynamoMap }
			defaultResults.any { it.id == 'default1' }
			defaultResults.any { it.data == 'test_data' }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should allow specifying class using as() method"() { // {{{
		given:
			String table = 'test_scan_as_method'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem(id: "as1", data: "typed_data"),
				new TestItem(id: "as2", data: "more_typed_data")
			]
			dynamo.putItems(table, items)

		when: 'Use scan and specify class with as() method'
			List<TestItem> typedResults = dynamo
				.scan(table)
				.as(TestItem)
				.list()

		then: 'Should return TestItem instances'
			typedResults.size() == 2
			typedResults.every { it instanceof TestItem }
			typedResults.any { it.id == 'as1' }
			typedResults.any { it.data == 'typed_data' }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

}
// vim: fdm=marker
