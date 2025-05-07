package it.grational.storage.dynamodb

// imports {{{
import spock.lang.*
import static java.util.Objects.*
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

class DynamoDbICSpec extends Specification {

	// fields {{{
	@Shared
	DynamoDb dynamoDb
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()
	// }}}

	def setupSpec() { // {{{
		dynamoDb = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)
	} // }}}

	def "Should insert and retrieve a single item with partition key"() { // {{{
		given:
			String table = 'test_items_pk'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'item1',
				data: 'test data'
			)
		and:
			DynamoKey key = new DynamoKey (
				item.impress(new DynamoMapper()).key()
			)

		when:
			dynamoDb.putItem (
				table,
				item
			)

		then:
			TestItem inserted = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
		and:
			inserted != null
			inserted.id == 'item1'
			inserted.data == 'test data'
			inserted.version == 1

		when: 'with another put the version is updated even with the same data'

			dynamoDb.putItem (
				table,
				inserted
			)
		then:
			TestItem versionUpdate = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
		and:
			versionUpdate != null
			versionUpdate.id == 'item1'
			versionUpdate.data == 'test data'
			versionUpdate.version == 2

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should insert and retrieve items with partition key and sort key"() { // {{{
		given:
			String table = 'test_items_composite'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamoDb.createTable (
				table,
				partKey,
				sortKey
			)
		and:
			TestItem firstItem = new TestItem (
				id: 'parent1',
				sortKey: 'child1',
				data: 'data1'
			)
		and:
			DynamoKey firstKey = new DynamoKey (
				firstItem.impress(new DynamoMapper()).key()
			)
		and:
			def items = [
				firstItem,
				new TestItem (
					id: 'parent1',
					sortKey: 'child2',
					data: 'data2'
				),
				new TestItem (
					id: 'parent1',
					sortKey: 'child3',
					data: 'data3'
				)
			]

		when:
			dynamoDb.putItems (
				table,
				items
			)

		then:
			TestItem retrieved = dynamoDb.getItem (
				table,
				firstKey,
				TestItem
			)
		and:
			retrieved != null
			retrieved.id == 'parent1'
			retrieved.sortKey == 'child1'
			retrieved.data == 'data1'
			retrieved.version == 1

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should handle versioning correctly during item puts and updates"() { // {{{
		given:
			String table = 'test_versioning'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'versioned',
				data: 'initial data'
			)
			DynamoKey key = new DynamoKey (
				item.impress(new DynamoMapper()).key()
			)

		when: 'first insertion'
			dynamoDb.putItem (
				table,
				item
			)

		then:
			def retrieved = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
			retrieved.id      == 'versioned'
			retrieved.data    == 'initial data'
			retrieved.version == 1

		when: 'update it'
			retrieved.data = 'updated data'
		and:
			dynamoDb.putItem (
				table,
				retrieved
			)

		then: 'data and version should be updated'
			def updated = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
			updated.id      == 'versioned'
			updated.data    == 'updated data'
			updated.version == 2

		when: 'simulate an outdated put'
			retrieved.data = 'conflict data'
			dynamoDb.putItem (
				table,
				retrieved
			)
		then: 'exception should be thrown'
			def exception = thrown (
				TransactionCanceledException
			)
			exception.message.contains (
				'ConditionalCheckFailed'
			)

		when: 'disabling the versioning allows overwriting'
			retrieved.data = 'forced data'
			dynamoDb.putItem (
				table,
				retrieved,
				false
			)

		then: 'data should be forcefully updated'
			def forced = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
			forced.id      == 'versioned'
			forced.data    == 'forced data'
			forced.version == 1

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should retrieve objects through secondary index"() { // {{{
		given:
			String table = 'test_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'data_index': 'tagField'
			]
		and:
			dynamoDb.createTable (
				table,
				partKey,
				null, // sortKey
				indexes
			)
		and:
			def items = [
				new TestItem(id:'idx1', tagField:'tag_a'),
				new TestItem(id:'idx2', tagField:'tag_a'),
				new TestItem(id:'idx3', tagField:'tag_b')
			]

		when:
			dynamoDb.putItems(table, items)

		then:
			List<TestItem> results = dynamoDb.query (
				table,
				'data_index',
				new DynamoKey('tagField', 'tag_a'),
				null,
				TestItem
			)
		and:
			results.size()     == 2
			results.first().id == 'idx1'
			results.last().id  == 'idx2'

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should retrieve objects through secondary index and filters"() { // {{{
		given:
			String table = 'contract_item_filters'
			String partKey = 'contract'
			String sortKey = 'sheet'
			String sharedOffer = '6f29bd2a-06b0-4a49-830d-6ae7871ab699'
			Map<String, String> indexes = [
				'offer-index': 'offer'
			]
		and:
			dynamoDb.createTable (
				table,
				partKey,
				sortKey,
				indexes
			)
		and:
			def items = [
				new ContractItem (
					contract:'B12345678',
					sheet: '001',
					offer: sharedOffer,
					data: 'data1',
					enabled: true
				),
				new ContractItem (
					contract:'A12345678',
					sheet: '001',
					offer: sharedOffer,
					data: 'data2',
					enabled: false
				),
				new ContractItem (
					contract:'C12345678',
					sheet: '001',
					offer: '6f29bd2a-06b0-4a49-830d-6ae7871ab100',
					data: 'data3',
					enabled: true
				)
			]

		when:
			dynamoDb.putItems(table, items)

		then:
			List<ContractItem> objects = dynamoDb.query (
				table,
				'offer-index',
				new DynamoKey('offer', sharedOffer),
				match('enabled', true),
				ContractItem
			)
		and:
			objects.size() == 1
			def first = objects.first()
		and:
			first.offer == sharedOffer
			first.contract == 'B12345678'
			first.sheet == '001'
			first.data == 'data1'
			first.enabled == true

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should delete items correctly"() { // {{{
		given:
			String table = 'test_delete'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'to_delete',
				data: 'to be deleted'
			)
		and:
			DynamoKey key = new DynamoKey (
				item.impress(new DynamoMapper()).key()
			)

		when:
			dynamoDb.putItem(table, item)

		then:
			TestItem exists = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
		and:
			exists != null

		when:
			dynamoDb.deleteItem(table, key)

		then:
			TestItem deleted = dynamoDb.getItem (
				table,
				key,
				TestItem
			)
		then:
			deleted == null

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should insert a large number of items (batch test)"() { // {{{
		given:
			String table = 'test_batch'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and:
			List<TestItem> items = (1..30).collect { int i ->
				new TestItem (
					id: "batch${i}",
					data: "batch data ${i}"
				)
			}

		when:
			dynamoDb.putItems(table, items)

		then:
			items.each { TestItem item ->
				TestItem retrieved = dynamoDb.getItem (
					table,
					new DynamoKey('id', item.id),
					TestItem
				)
				assert retrieved         != null
				assert retrieved.id      == item.id
				assert retrieved.data    == item.data
				assert retrieved.version == 1
			}

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should handle tables with composite keys and composite indexes correctly"() { // {{{{
		given:
			String table = 'test_composite'
			String partKey = 'id'
			String sortKey = 'sortKey'
			Map<String, String> indexes = [
				'tag_index': 'tagField'
			]
		and:
			dynamoDb.createTable (
				table,
				partKey,
				sortKey,
				indexes
			)
		and:
			TestItem first = new TestItem (
				id: 'pk1',
				sortKey: 'sk1',
				tagField: 'tag1',
				data: 'c1'
			)
		and:
			DynamoKey firstKey = new DynamoKey (
				first.impress(new DynamoMapper()).key()
			)

		and:
			List<TestItem> items = [
				first,
				new TestItem (
					id: 'pk1',
					sortKey: 'sk2',
					tagField: 'tag2',
					data: 'c2'
				),
				new TestItem (
					id: 'pk2',
					sortKey: 'sk1',
					tagField: 'tag1',
					data: 'c3'
				)
			]

		when:
			dynamoDb.putItems (
				table,
				items
			)

		then:
			TestItem retrieved = dynamoDb.getItem (
				table,
				firstKey,
				TestItem
			)
			retrieved          != null
			retrieved.tagField == 'tag1'
			retrieved.data     == 'c1'
			retrieved.version  == 1

		when:
			List<TestItem> results = dynamoDb.query (
				table,
				'tag_index',
				new DynamoKey('tagField', 'tag1'),
				null,
				TestItem
			)
		then:
			results.size() == 2
			results.any {
				it.id       == 'pk1' &&
				it.sortKey  == 'sk1' &&
				it.tagField == 'tag1' &&
				it.version  == 1
			}
			results.any {
				it.id       == 'pk2' &&
				it.sortKey  == 'sk1' &&
				it.tagField == 'tag1' &&
				it.version  == 1
			}

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should be capable of querying objects only by their partition key"() { // {{{{
		given:
			String table = 'test_partition_key'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamoDb.createTable (
				table,
				partKey,
				sortKey
			)
		and:
			List<TestItem> items = [
				new TestItem (
					id: 'pk1',
					sortKey: 'sk1',
					data: 'c1'
				),
				new TestItem (
					id: 'pk1',
					sortKey: 'sk2',
					data: 'c2'
				),
				new TestItem (
					id: 'pk2',
					sortKey: 'sk3',
					data: 'c3'
				)
			]
		and:
			dynamoDb.putItems (
				table,
				items
			)

		when:
			List<TestItem> results = dynamoDb.query (
				table,
				new DynamoKey('id', 'pk1'),
				null,
				TestItem
			)
		then:
			results.size() == 2
			results.any {
				it.id       == 'pk1' &&
				it.sortKey  == 'sk1' &&
				it.data     == 'c1' &&
				it.version  == 1
			}
			results.any {
				it.id       == 'pk1' &&
				it.sortKey  == 'sk2' &&
				it.data     == 'c2' &&
				it.version  == 1
			}

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should be able to update an item"() { // {{{
		given:
			String table = 'test_update'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey,
			)
		and:
			TestItem item = new TestItem (
				id: 'pk1',
				tagField: 'tag1',
				data: 'c1'
			)
		and:
			DynamoKey key = new DynamoKey (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamoDb.putItem (
				table,
				item
			)

		expect:
			TestItem unmodified = dynamoDb.getItem (
				table, key, TestItem
			)
			unmodified          != null
			unmodified.tagField == 'tag1'
			unmodified.data     == 'c1'
			unmodified.version  == 1

		when: 'try a non-versioned update omitting the version field in the mapper'
			DynamoMapper mapper = new DynamoMapper()
				.with('id', 'pk1', FieldType.PARTITION_KEY)
				.with('data', 'updated')
		and:
			dynamoDb.updateItem (
				table,
				mapper
			)
		then:
			noExceptionThrown()
		and:
			TestItem updated = dynamoDb.getItem (
				table, key, TestItem
			)
			updated          != null
			updated.tagField == 'tag1'
			updated.data     == 'updated'
			updated.version  == 1

		when: 'a (failing) versioned update with a non corresponding version'
			mapper = new DynamoMapper()
				.with('id', 'pk1', FieldType.PARTITION_KEY)
				.with('data', 'not to be written')
				.with('version', 54, FieldType.VERSION)
		and:
			dynamoDb.updateItem (
				table,
				mapper
			)
		then: 'exception should be thrown'
			def exception = thrown(ConditionalCheckFailedException)
			exception.message.startsWith('The conditional request failed')
		and:
			TestItem untouched = dynamoDb.getItem (
				table, key, TestItem
			)
			untouched          != null
			untouched.tagField == 'tag1'
			untouched.data     == 'updated'
			untouched.version  == 1

		when: 'we provide the corresponding version'
			mapper = new DynamoMapper()
				.with('id', 'pk1', FieldType.PARTITION_KEY)
				.with('data', 'these are ok!')
				.with('version', 1, FieldType.VERSION)
		and:
			dynamoDb.updateItem (
				table,
				mapper
			)
		then:
			TestItem versioned = dynamoDb.getItem (
				table, key, TestItem
			)
			versioned          != null
			versioned.tagField == 'tag1'
			versioned.data     == 'these are ok!'
			versioned.version  == 2


		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should scan table and return filtered results"() { // {{{
		setup:
			String table = 'test_scan'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and:
			List<TestItem> items = [
				new TestItem(id: 'scan1', tagField: 'category_a', data: 'data1', enabled: true),
				new TestItem(id: 'scan2', tagField: 'category_a', data: 'data2', enabled: false),
				new TestItem(id: 'scan3', tagField: 'category_b', data: 'data3', enabled: true),
				new TestItem(id: 'scan4', tagField: 'category_b', data: 'data4', enabled: false),
				new TestItem(id: 'scan5', tagField: 'category_c', data: 'data5', enabled: true)
			]
		and:
			dynamoDb.putItems(table, items)

		when:
			List<TestItem> allResults = dynamoDb.scan (
				table,
				null,
				TestItem
			)
		then:
			allResults.size() == 5
			allResults.collect { it.id } ==~ ['scan1', 'scan2', 'scan3', 'scan4', 'scan5']

		when:
			List<TestItem> enabledResults = dynamoDb.scan (
				table,
				match('enabled', true),
				TestItem
			)
		then:
			enabledResults.size() == 3
			enabledResults.every { it.enabled }
			enabledResults.collect { it.id } ==~ ['scan1', 'scan3', 'scan5']

		when:
			List<TestItem> complexResults = dynamoDb.scan (
				table,
				every (
					match('tagField', 'category_a'),
					match('enabled', true)
				),
				TestItem,
			)
		then:
			complexResults.size() == 1
			complexResults.first().id == 'scan1'
			complexResults.first().tagField == 'category_a'
			complexResults.first().enabled == true

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should delete multiple items by key and filter"() { // {{{
		setup:
			String table = 'test_bulk_delete'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and: 'Creating a batch of items to delete'
			List<TestItem> items = (1..15).collect { int i ->
				new TestItem (
					id: "del${i}",
					tagField: "cat_${(i % 3) + 1}", // 2,3,1, 2,3,1...
					data: "data${i}",
					enabled: (i % 2 == 0) // false if odd
				)
			}
		and:
			dynamoDb.putItems(table, items)

		when: 'Verifying items were inserted'
			List<TestItem> allItems = dynamoDb.scan (
				table,
				null,
				TestItem
			)
		then:
			allItems.size() == 15

		when: 'Deleting items with partition key only'
			int deletedCount = dynamoDb.deleteItems (
				table,
				new DynamoKey('id', 'del1')
			)
		then:
			deletedCount == 1

		when: 'Checking item was deleted'
			TestItem shouldBeDeleted = dynamoDb.getItem (
				table,
				new DynamoKey('id', 'del1'),
				TestItem
			)
		then:
			shouldBeDeleted == null

		when: 'Mass deleting items with a filter'
			int filterDeleteCount = dynamoDb.deleteItems (
				table,
				match('tagField', 'cat_2')
			)
		then:
			filterDeleteCount == 4  // 5 items with tagField='cat_1' but one was already deleted

		when: 'Verifying remaining items'
			List<TestItem> remaining = dynamoDb.scan (
				table,
				null,
				TestItem
			)
		then:
			remaining.size() == 10
			remaining.every { it.tagField != 'cat_2' }

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should delete multiple items by index and filter"() { // {{{
		setup:
			String table = 'test_delete_by_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'tag_index': 'tagField'
			]
		and:
			dynamoDb.createTable (
				table,
				partKey,
				null,  // no sort key
				indexes
			)
		and:
			List<TestItem> items = [
				new TestItem(id: 'idx1', tagField: 'tag_a', data: 'data1', enabled: true),
				new TestItem(id: 'idx2', tagField: 'tag_a', data: 'data2', enabled: false),
				new TestItem(id: 'idx3', tagField: 'tag_b', data: 'data3', enabled: true),
				new TestItem(id: 'idx4', tagField: 'tag_b', data: 'data4', enabled: false),
				new TestItem(id: 'idx5', tagField: 'tag_c', data: 'data5', enabled: true)
			]
		and:
			dynamoDb.putItems(table, items)

		when: 'Deleting items using an index'
			int deleteCount = dynamoDb.deleteItems (
				table,
				'tag_index',
				new DynamoKey('tagField', 'tag_a'),
				null
			)
		then:
			deleteCount == 2

		when: 'Verifying remaining items'
			List<TestItem> remaining = dynamoDb.scan (
				table,
				null,
				TestItem
			)
		then:
			remaining.size() == 3
			remaining.every { it.tagField != 'tag_a' }

		when: 'Deleting with index and additional filter'
			int filteredDeleteCount = dynamoDb.deleteItems (
				table,
				'tag_index',
				new DynamoKey('tagField', 'tag_b'),
				match('enabled', true)
			)
		then:
			filteredDeleteCount == 1

		when: 'Verifying final remaining items'
			List<TestItem> finalRemaining = dynamoDb.scan (
				table,
				null,
				TestItem
			)
		then:
			finalRemaining ==~ items.findAll { it.id in [ 'idx4', 'idx5' ] }

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	def "Should return PagedResult when pagination parameters are used"() {
		given:
			String table = 'test_paged'
			dynamoDb.createTable(table, 'id', 'sortKey')
			Integer totalSize = 20
			Integer pageSize = 5
		and:
			List<TestItem> items = (1..totalSize).collect { 
				new TestItem(id: "user1", sortKey: "key${it}")
			}
			dynamoDb.putItems(table, items)

		when: 'Using limit parameter'
			PagedResult<TestItem> first = dynamoDb.query (
				table,
				null,
				new DynamoKey('id', 'user1'),
				null,
				TestItem.class,
				pageSize
			)

		then:
			first.count == 5
			first.more == true

		when: 'Using last parameter'
			PagedResult<TestItem> second = dynamoDb.query (
				table,
				null,
				new DynamoKey('id', 'user1'),
				null,
				TestItem.class,
				totalSize, // more than the rest
				first.last
			)

		then:
			second.count == 15
			second.more == false

		cleanup:
			dynamoDb.dropTable(table)
	}

	def "Should handle scanIndexForward for query ordering"() {
		given:
			String table = 'test_scan_order'
			String partKey = 'id'
			String sortKey = 'sortKey'
			
			dynamoDb.createTable(table, partKey, sortKey)
			
			List<TestItem> items = [
				new TestItem(id: 'user1', sortKey: '2025-01-01'),
				new TestItem(id: 'user1', sortKey: '2025-01-02'),
				new TestItem(id: 'user1', sortKey: '2025-01-03')
			]
			
			dynamoDb.putItems(table, items)

		when: 'Query with forward order'
			List<TestItem> ascending = dynamoDb.query (
				table,
				new DynamoKey('id', 'user1'),
				null,
				TestItem,
				true
			)
			
		then:
			ascending[0].sortKey == '2025-01-01'
			ascending[1].sortKey == '2025-01-02'
			ascending[2].sortKey == '2025-01-03'

		when: 'Query with backward order'
			List<TestItem> descending = dynamoDb.query(
				table,
				new DynamoKey('id', 'user1'),
				null,
				TestItem,
				false
			)
			
		then:
			descending[0].sortKey == '2025-01-03'
			descending[1].sortKey == '2025-01-02'
			descending[2].sortKey == '2025-01-01'

		cleanup:
			dynamoDb.dropTable(table)
	}

	@Ignore
	// Both these options are ignored in the local version of DynamoDB
	// see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html
	def "Should perform parallel and limited scan operations"() { // {{{
		given:
			String table = 'test_less_common_scan'
			String partKey = 'id'
		and:
			dynamoDb.createTable (
				table,
				partKey
			)
		and: 'Create a larger dataset for parallel scanning'
			List<TestItem> items = (1..50).collect { int i ->
				new TestItem (
					id: "item${i}",
					tagField: "category_${(i % 5) + 1}",
					data: "data${i}",
					enabled: (i % 2 == 0)
				)
			}
		and:
			dynamoDb.putItems(table, items)

		when: 'Performing a parallel scan with 2 segments'
			List<TestItem> segment0Results = dynamoDb.scan (
				table,
				null,   // No filter
				TestItem,
				null,   // No limit
				0,      // Segment 0
				2       // Total of 2 segments
			)

			List<TestItem> segment1Results = dynamoDb.scan (
				table,
				null,   // No filter
				TestItem,
				null,   // No limit
				1,      // Segment 1
				2       // Total of 2 segments
			)

		then: 'The combined results should contain all items'
			def combinedResults = segment0Results + segment1Results
			combinedResults.size() == items.size()

			// Check that the segments don't overlap and contain all items
			def segmentIds = segment0Results*.id + segment1Results*.id
			segmentIds.sort() == items*.id.sort()

		when: 'Performing a filtered parallel scan'
			DynamoFilter enabledFilter = match('enabled', true)

			List<TestItem> filteredSegment0 = dynamoDb.scan (
				table,
				enabledFilter,
				TestItem,
				null,   // No limit
				0,      // Segment 0
				2       // Total of 2 segments
			)

			List<TestItem> filteredSegment1 = dynamoDb.scan (
				table,
				enabledFilter,
				TestItem,
				null,   // No limit
				1,      // Segment 1
				2       // Total of 2 segments
			)

		then: 'The combined filtered results should contain only enabled items'
			def combinedFiltered = filteredSegment0 + filteredSegment1
			combinedFiltered.every { it.enabled }
			combinedFiltered.size() == items.count { it.enabled }

		when: 'scanning with a limit'
			List<TestItem> limitedResults = dynamoDb.scan (
				table,
				null,
				TestItem,
				2
			)

		then: 'only the specified number of items should be returned'
			limitedResults.size() <= 2

		cleanup:
			dynamoDb.dropTable(table)
	} // }}}

	static class TestItem // {{{
		implements Storable<AttributeValue,String> {
		String id
		String sortKey
		String contract
		String sheet
		String offer
		String data
		String tagField
		boolean enabled = false
		Integer version = 0

		@Override
		DbMapper<AttributeValue,String> impress (
			DbMapper<AttributeValue,String> mapper,
			boolean versioned = true
		) {
			mapper.with (
				'id', id,
				FieldType.PARTITION_KEY
			)

			if (sortKey)
				mapper.with (
					'sortKey', sortKey,
					FieldType.SORT_KEY
				)

			if (data)
				mapper.with('data', data)

			if (tagField)
				mapper.with('tagField', tagField)

			mapper.with('enabled', enabled)

			if ( nonNull(version) )
				mapper.with (
					'version', version,
					versioned
						? FieldType.VERSION
						: FieldType.STANDARD
				)
			return mapper
		}

		@Override
		String toString() {
			"TestItem(id: $id, sortKey: $sortKey, data: $data, enabled: $enabled, version: $version)"
		}

		boolean equals(o) {
			if (this.is(o)) return true
			if (!(o instanceof TestItem)) return false

			TestItem testItem = (TestItem) o

			if (data != testItem.data) return false
			if (id != testItem.id) return false
			if (sortKey != testItem.sortKey) return false
			if (tagField != testItem.tagField) return false
			if (enabled != testItem.enabled) return false

			return true
		}
	} // }}}

	@ToString (
		includePackage=false,
		includeFields=true,
		includeNames=true
	)
	@EqualsAndHashCode
	static class ContractItem // {{{
		implements Storable<AttributeValue,String> {
		String contract
		String sheet
		String offer
		String data
		boolean enabled = false
		Integer version = 0

		@Override
		DbMapper<AttributeValue,String> impress (
			DbMapper<AttributeValue,String> mapper,
			boolean versioned = true
		) {
			mapper.with (
				'contract', contract,
				FieldType.PARTITION_KEY
			)

			mapper.with (
				'sheet', sheet,
				FieldType.SORT_KEY
			)

			mapper.with('offer', offer)

			if (data)
				mapper.with('data', data)

			mapper.with('enabled', enabled)

			if ( nonNull(version) )
				mapper.with (
					'version', version,
					versioned
						? FieldType.VERSION
						: FieldType.STANDARD
				)
			return mapper
		}

	} // }}}
}
// vim: fdm=marker
