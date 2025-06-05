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
	DynamoDb dynamo
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()
	// }}}

	def setupSpec() { // {{{
		dynamo = new DynamoDb (
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
			dynamo.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'item1',
				data: 'test data'
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)

		when:
			dynamo.putItem (
				table,
				item
			)

		then:
			TestItem inserted = dynamo.getItem (
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

			dynamo.putItem (
				table,
				inserted
			)
		then:
			TestItem versionUpdate = dynamo.getItem (
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
			dynamo.dropTable(table)
	} // }}}

	def "Should insert and retrieve items with partition key and sort key"() { // {{{
		given:
			String table = 'test_items_composite'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable (
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
			KeyFilter firstKey = KeyFilter.of (
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
			dynamo.putItems (
				table,
				items
			)

		then:
			TestItem retrieved = dynamo.getItem (
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
			dynamo.dropTable(table)
	} // }}}

	def "Should handle versioning correctly during item puts and updates"() { // {{{
		given:
			String table = 'test_versioning'
			String partKey = 'id'
		and:
			dynamo.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'versioned',
				data: 'initial data'
			)
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)

		when: 'first insertion'
			dynamo.putItem (
				table,
				item
			)

		then:
			def retrieved = dynamo.getItem (
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
			dynamo.putItem (
				table,
				retrieved
			)

		then: 'data and version should be updated'
			def updated = dynamo.getItem (
				table,
				key,
				TestItem
			)
			updated.id      == 'versioned'
			updated.data    == 'updated data'
			updated.version == 2

		when: 'simulate an outdated put'
			retrieved.data = 'conflict data'
			dynamo.putItem (
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
			dynamo.putItem (
				table,
				retrieved,
				false
			)

		then: 'data should be forcefully updated'
			def forced = dynamo.getItem (
				table,
				key,
				TestItem
			)
			forced.id      == 'versioned'
			forced.data    == 'forced data'
			forced.version == 1

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should retrieve objects through secondary index"() { // {{{
		given:
			String table = 'test_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'data_index': 'tagField'
			]
		and:
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		then:
			List<TestItem> results = dynamo.query (
				table, 
				'data_index',
				KeyFilter.of('tagField', 'tag_a'),
				TestItem
			)
			.list()
		and:
			results.size()     == 2
			results.first().id == 'idx1'
			results.last().id  == 'idx2'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should retrieve objects through secondary index with sort key"() { // {{{
		given:
			String table = 'test_index_sort_key'
			Scalar partKey = Scalar.of('id')
			Index compositeIndex = Index.of (
				'tagField',
				'sortKey',
				'data_index'
			)
		and:
			dynamo.createTable (
				table,
				partKey,
				Optional.empty(), // primary table sortKey
				compositeIndex
			)
		and:
			def items = [
				new TestItem(id:'idx1', tagField:'tag_a', sortKey:'sort1'),
				new TestItem(id:'idx2', tagField:'tag_a', sortKey:'sort2'),
				new TestItem(id:'idx3', tagField:'tag_a', sortKey:'sort3'),
				new TestItem(id:'idx4', tagField:'tag_b', sortKey:'sort1')
			]

		when:
			dynamo.putItems(table, items)

		then: "Can query by partition key only"
			List<TestItem> results = dynamo.query (
				table,
				'data_index',
				KeyFilter.of('tagField', 'tag_a'),
				TestItem
			)
			.list()
		and:
			results.size() == 3
			results*.id ==~ ['idx1', 'idx2', 'idx3']
			results*.sortKey.sort() == ['sort1', 'sort2', 'sort3']

		and: "Can query by partition and sort key"
			List<TestItem> specificResult = dynamo.query (
				table,
				'data_index',
				KeyFilter.of('tagField', 'tag_a', 'sortKey', 'sort2'),
				TestItem
			)
			.list()
		and:
			specificResult.size() == 1
			specificResult.first().id == 'idx2'
			specificResult.first().sortKey == 'sort2'

		cleanup:
			dynamo.dropTable(table)
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
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		then:
			List<ContractItem> objects = dynamo.query (
				table,
				'offer-index',
				KeyFilter.of('offer', sharedOffer),
				ContractItem
			)
			.filter(match('enabled', true))
			.list()
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
			dynamo.dropTable(table)
	} // }}}

	def "Should retrieve objects through secondary index with sort key and filter"() { // {{{
		given:
			String table = 'composite_index_filter'
			Scalar partKey = Scalar.of('id')
			Scalar sortKey = Scalar.of('sortKey')
			Index compositeIndex = Index.of (
				'tagField',
				'data',
				'tag-data-index'
			)
		and:
			dynamo.createTable (
				table,
				partKey,
				Optional.of(sortKey),
				compositeIndex
			)
		and:
			def items = [
				new TestItem (
					id: 'item1',
					sortKey: '2023-01-01',
					tagField: 'category_a',
					data: 'high',
					enabled: true
				),
				new TestItem (
					id: 'item2',
					sortKey: '2023-01-02',
					tagField: 'category_a',
					data: 'medium',
					enabled: false
				),
				new TestItem (
					id: 'item3',
					sortKey: '2023-01-03',
					tagField: 'category_a',
					data: 'low',
					enabled: true
				),
				new TestItem (
					id: 'item4',
					sortKey: '2023-01-04',
					tagField: 'category_b',
					data: 'high',
					enabled: true
				)
			]

		when:
			dynamo.putItems(table, items)

		then: "Can query with composite index and filter"
			List<TestItem> results = dynamo.query (
				table,
				'tag-data-index',
				KeyFilter.of (
					'tagField', 'category_a',
					'data', 'high'
				),
				TestItem
			)
			.filter(match('enabled', true))
			.list()

		and:
			results.size() == 1
			results.first().id == 'item1'
			results.first().tagField == 'category_a'
			results.first().data == 'high'
			results.first().enabled == true

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should delete items correctly"() { // {{{
		given:
			String table = 'test_delete'
			String partKey = 'id'
		and:
			dynamo.createTable (
				table,
				partKey
			)
		and:
			 TestItem item = new TestItem (
				id: 'to_delete',
				data: 'to be deleted'
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)

		when:
			dynamo.putItem(table, item)

		then:
			TestItem exists = dynamo.getItem (
				table,
				key,
				TestItem
			)
		and:
			exists != null

		when:
			dynamo.deleteItem(table, key)

		then:
			TestItem deleted = dynamo.getItem (
				table,
				key,
				TestItem
			)
		then:
			deleted == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should insert a large number of items (batch test)"() { // {{{
		given:
			String table = 'test_batch'
			String partKey = 'id'
		and:
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		then:
			items.each { TestItem item ->
				TestItem retrieved = dynamo.getItem (
					table,
					KeyFilter.of('id', item.id),
					TestItem
				)
				assert retrieved         != null
				assert retrieved.id      == item.id
				assert retrieved.data    == item.data
				assert retrieved.version == 1
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle tables with composite keys and composite indexes correctly"() { // {{{{
		given:
			String table = 'test_composite'
			Scalar partKey = Scalar.of('id')
			Optional<Scalar> sortKey = Optional.of(Scalar.of('sortKey'))
			Index simpleIdx = Index.of('tagField')
			Index compositeIdx = Index.of('tagField', 'data')
		and:
			dynamo.createTable (
				table,
				partKey,
				sortKey,
				simpleIdx,
				compositeIdx
			)
		and:
			TestItem first = new TestItem (
				id: 'pk1',
				sortKey: 'sk1',
				tagField: 'tag1',
				data: 'c1'
			)
		and:
			KeyFilter firstKey = KeyFilter.of (
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
			dynamo.putItems (
				table,
				items
			)

		then:
			TestItem retrieved = dynamo.getItem (
				table,
				firstKey,
				TestItem
			)
			retrieved          != null
			retrieved.tagField == 'tag1'
			retrieved.data     == 'c1'
			retrieved.version  == 1

		when: "Query using simple index"
			List<TestItem> results = dynamo.query (
				table,
				'tagField-index',
				KeyFilter.of('tagField', 'tag1'),
				TestItem
			)
			.list()
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

		when: "Query using composite index with partition key only"
			List<TestItem> compositeResults = dynamo.query (
				table,
				'tagField-data-index',
				KeyFilter.of('tagField', 'tag1'),
				TestItem
			)
			.list()
		then:
			compositeResults.size() == 2
			compositeResults.every { it.tagField == 'tag1' }
			compositeResults*.data.sort() == ['c1', 'c3']

		when: "Query using composite index with both partition and sort keys"
			List<TestItem> specificResults = dynamo.query (
				table,
				'tagField-data-index',
				KeyFilter.of('tagField', 'tag1', 'data', 'c1'),
				TestItem
			)
			.list()
		then:
			specificResults.size() == 1
			specificResults.first().id == 'pk1'
			specificResults.first().sortKey == 'sk1'
			specificResults.first().data == 'c1'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should be capable of querying objects only by their partition key"() { // {{{{
		given:
			String table = 'test_partition_key'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable (
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
			dynamo.putItems (
				table,
				items
			)

		when:
			List<TestItem> results = dynamo.query (
				table,
				KeyFilter.of('id', 'pk1'),
				TestItem
			).list()
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
			dynamo.dropTable(table)
	} // }}}

	def "Should be able to update an item"() { // {{{
		given:
			String table = 'test_update'
			String partKey = 'id'
		and:
			dynamo.createTable (
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
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamo.putItem (
				table,
				item
			)

		expect:
			TestItem unmodified = dynamo.getItem (
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
			dynamo.updateItem (
				table,
				mapper
			)
		then:
			noExceptionThrown()
		and:
			TestItem updated = dynamo.getItem (
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
			dynamo.updateItem (
				table,
				mapper
			)
		then: 'exception should be thrown'
			def exception = thrown(ConditionalCheckFailedException)
			exception.message.startsWith('The conditional request failed')
		and:
			TestItem untouched = dynamo.getItem (
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
			dynamo.updateItem (
				table,
				mapper
			)
		then:
			TestItem versioned = dynamo.getItem (
				table, key, TestItem
			)
			versioned          != null
			versioned.tagField == 'tag1'
			versioned.data     == 'these are ok!'
			versioned.version  == 2


		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan table and return filtered results"() { // {{{
		setup:
			String table = 'test_scan'
			String partKey = 'id'
		and:
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		when:
			List<TestItem> allResults = dynamo
				.scan(table, TestItem)
				.list()
		then:
			allResults.size() == 5
			allResults.collect { it.id } ==~ ['scan1', 'scan2', 'scan3', 'scan4', 'scan5']

		when:
			List<TestItem> enabledResults = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.list()
		then:
			enabledResults.size() == 3
			enabledResults.every { it.enabled }
			enabledResults.collect { it.id } ==~ ['scan1', 'scan3', 'scan5']

		when:
			List<TestItem> complexResults = dynamo
				.scan(table, TestItem)
				.filter (
					every (
						match('tagField', 'category_a'),
						match('enabled', true)
					)
				).list()
		then:
			complexResults.size() == 1
			complexResults.first().id == 'scan1'
			complexResults.first().tagField == 'category_a'
			complexResults.first().enabled == true

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should delete multiple items by key and filter"() { // {{{
		setup:
			String table = 'test_bulk_delete'
			String partKey = 'id'
		and:
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		when: 'Verifying items were inserted'
			List<TestItem> allItems = dynamo
				.scan(table, TestItem)
				.list()
		then:
			allItems.size() == 15

		when: 'Deleting items with partition key only'
			int deletedCount = dynamo.deleteItems (
				table,
				KeyFilter.of('id', 'del1')
			)
		then:
			deletedCount == 1

		when: 'Checking item was deleted'
			TestItem shouldBeDeleted = dynamo.getItem (
				table,
				KeyFilter.of('id', 'del1'),
				TestItem
			)
		then:
			shouldBeDeleted == null

		when: 'Mass deleting items with a filter'
			int filterDeleteCount = dynamo.deleteItems (
				table,
				match('tagField', 'cat_2')
			)
		then:
			filterDeleteCount == 4  // 5 items with tagField='cat_1' but one was already deleted

		when: 'Verifying remaining items'
			List<TestItem> remaining = dynamo
				.scan(table, TestItem)
				.list()
		then:
			remaining.size() == 10
			remaining.every { it.tagField != 'cat_2' }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should delete multiple items by index and filter"() { // {{{
		setup:
			String table = 'test_delete_by_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'tag_index': 'tagField'
			]
		and:
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		when: 'Deleting items using an index'
			int deleteCount = dynamo.deleteItems (
				table,
				'tag_index',
				KeyFilter.of('tagField', 'tag_a'),
				null
			)
		then:
			deleteCount == 2

		when: 'Verifying remaining items'
			List<TestItem> remaining = dynamo
				.scan(table, TestItem)
				.list()
		then:
			remaining.size() == 3
			remaining.every { it.tagField != 'tag_a' }

		when: 'Deleting with index and additional filter'
			int filteredDeleteCount = dynamo.deleteItems (
				table,
				'tag_index',
				KeyFilter.of('tagField', 'tag_b'),
				match('enabled', true)
			)
		then:
			filteredDeleteCount == 1

		when: 'Verifying final remaining items'
			List<TestItem> finalRemaining = dynamo
				.scan(table, TestItem)
				.list()
		then:
			finalRemaining ==~ items.findAll { it.id in [ 'idx4', 'idx5' ] }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should return PagedResult when pagination parameters are used"() { // {{{
		given:
			String table = 'test_paged'
			dynamo.createTable(table, 'id', 'sortKey')
			Integer totalSize = 20
			Integer pageSize = 5
		and:
			List<TestItem> items = (1..totalSize).collect {
				new TestItem(id: "user1", sortKey: "key${it}")
			}
			dynamo.putItems(table, items)

		when: 'Using limit parameter'
			PagedResult<TestItem> first = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			).paged(pageSize)

		then:
			first.count == 5
			first.more == true

		when: 'Using last parameter'
			PagedResult<TestItem> second = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			)
			.paged(totalSize, first.last, true)

		then:
			second.count == 15
			second.more == false

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle scanIndexForward for query ordering"() { // {{{
		given:
			String table = 'test_scan_order'
			String partKey = 'id'
			String sortKey = 'sortKey'

			dynamo.createTable(table, partKey, sortKey)

			List<TestItem> items = [
				new TestItem(id: 'user1', sortKey: '2025-01-01'),
				new TestItem(id: 'user1', sortKey: '2025-01-02'),
				new TestItem(id: 'user1', sortKey: '2025-01-03')
			]

			dynamo.putItems(table, items)

		when: 'Query with forward order'
			List<TestItem> ascending = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			)
			.forward()
			.list()

		then:
			ascending[0].sortKey == '2025-01-01'
			ascending[1].sortKey == '2025-01-02'
			ascending[2].sortKey == '2025-01-03'

		when: 'Query with backward order'
			List<TestItem> descending = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			)
			.backward()
			.list()

		then:
			descending[0].sortKey == '2025-01-03'
			descending[1].sortKey == '2025-01-02'
			descending[2].sortKey == '2025-01-01'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should create table with partition key and array of Index objects"() { // {{{
		given:
			String table = 'test_index_array'
			String partKey = 'id'
			Index[] indexes = [
				Index.of('email'),
				Index.of('status', 'createdAt', 'custom-status-index')
			]

		when:
			dynamo.createTable (
				table,
				partKey,
				indexes
			)

		then:
			def description = dynamo.client.describeTable (
				DescribeTableRequest.builder()
					.tableName(table)
					.build()
			).table()

			// Check primary key schema
			description.keySchema().size() == 1
			description.keySchema()[0].attributeName() == 'id'
			description.keySchema()[0].keyType() == KeyType.HASH

			// Check secondary indexes
			description.globalSecondaryIndexes().size() == 2
			
			// Check first index
			def emailIndex = description.globalSecondaryIndexes().find { it.indexName() == 'email-index' }
			emailIndex != null
			emailIndex.keySchema().size() == 1
			emailIndex.keySchema()[0].attributeName() == 'email'
			emailIndex.keySchema()[0].keyType() == KeyType.HASH
			
			// Check second index
			def statusIndex = description.globalSecondaryIndexes().find { it.indexName() == 'custom-status-index' }
			statusIndex != null
			statusIndex.keySchema().size() == 2
			statusIndex.keySchema()[0].attributeName() == 'status'
			statusIndex.keySchema()[0].keyType() == KeyType.HASH
			statusIndex.keySchema()[1].attributeName() == 'createdAt'
			statusIndex.keySchema()[1].keyType() == KeyType.RANGE

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should create table with partition key, sort key and array of Index objects"() { // {{{
		given:
			String table = 'test_index_array_with_sort'
			String partKey = 'id'
			String sortKey = 'timestamp'
			Index[] indexes = [
				Index.of('category', 'price'),
				Index.of('status')
			]

		when:
			dynamo.createTable (
				table,
				partKey,
				sortKey,
				indexes
			)

		then:
			def description = dynamo.client.describeTable (
				DescribeTableRequest.builder()
					.tableName(table)
					.build()
			).table()

			// Check primary key schema
			description.keySchema().size() == 2
			description.keySchema().find { it.attributeName() == 'id' && it.keyType() == KeyType.HASH }
			description.keySchema().find { it.attributeName() == 'timestamp' && it.keyType() == KeyType.RANGE }

			// Check secondary indexes
			description.globalSecondaryIndexes().size() == 2
			
			// Check indexes
			def categoryIndex = description.globalSecondaryIndexes().find { it.indexName() == 'category-price-index' }
			categoryIndex != null
			categoryIndex.keySchema().size() == 2
			categoryIndex.keySchema()[0].attributeName() == 'category'
			categoryIndex.keySchema()[0].keyType() == KeyType.HASH
			categoryIndex.keySchema()[1].attributeName() == 'price'
			categoryIndex.keySchema()[1].keyType() == KeyType.RANGE
			
			def statusIndex = description.globalSecondaryIndexes().find { it.indexName() == 'status-index' }
			statusIndex != null
			statusIndex.keySchema().size() == 1
			statusIndex.keySchema()[0].attributeName() == 'status'
			statusIndex.keySchema()[0].keyType() == KeyType.HASH

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should create table with Scalar partition key and array of Index objects"() { // {{{
		given:
			String table = 'test_scalar_partition'
			Scalar partKey = Scalar.of('id', ScalarAttributeType.S)
			Index[] indexes = [
				Index.of(
					Scalar.of('email', ScalarAttributeType.S)
				),
				Index.of(
					Scalar.of('age', ScalarAttributeType.N)
				)
			]

		when:
			dynamo.createTable (
				table,
				partKey,
				Optional.empty(),
				indexes
			)

		then:
			def description = dynamo.client.describeTable (
				DescribeTableRequest.builder()
					.tableName(table)
					.build()
			).table()

			// Check primary key schema
			description.keySchema().size() == 1
			description.keySchema()[0].attributeName() == 'id'
			description.keySchema()[0].keyType() == KeyType.HASH

			// Check attribute definitions
			def idAttr = description.attributeDefinitions().find { it.attributeName() == 'id' }
			idAttr != null
			idAttr.attributeType() == ScalarAttributeType.S

			def emailAttr = description.attributeDefinitions().find { it.attributeName() == 'email' }
			emailAttr != null
			emailAttr.attributeType() == ScalarAttributeType.S

			def ageAttr = description.attributeDefinitions().find { it.attributeName() == 'age' }
			ageAttr != null
			ageAttr.attributeType() == ScalarAttributeType.N

			// Check secondary indexes
			description.globalSecondaryIndexes().size() == 2

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should create table with Scalar partition key, sort key and array of Index objects"() { // {{{
		given:
			String table = 'test_scalar_composite'
			Scalar partKey = Scalar.of('id', ScalarAttributeType.S)
			Scalar sortKey = Scalar.of('timestamp', ScalarAttributeType.N)
			Index[] indexes = [
				Index.of(
					Scalar.of('category', ScalarAttributeType.S),
					Scalar.of('price', ScalarAttributeType.N),
					'cat-price-index'
				),
				Index.of(
					Scalar.of('status', ScalarAttributeType.S),
					Scalar.of('count', ScalarAttributeType.N)
				)
			]

		when:
			dynamo.createTable(
				table,
				partKey,
				Optional.of(sortKey),
				indexes
			)

		then:
			def description = dynamo.client.describeTable(
				DescribeTableRequest.builder()
					.tableName(table)
					.build()
			).table()

			// Check primary key schema
			description.keySchema().size() == 2
			description.keySchema().find { it.attributeName() == 'id' && it.keyType() == KeyType.HASH }
			description.keySchema().find { it.attributeName() == 'timestamp' && it.keyType() == KeyType.RANGE }

			// Check attribute definitions
			def idAttr = description.attributeDefinitions().find { it.attributeName() == 'id' }
			idAttr != null
			idAttr.attributeType() == ScalarAttributeType.S

			def timestampAttr = description.attributeDefinitions().find { it.attributeName() == 'timestamp' }
			timestampAttr != null
			timestampAttr.attributeType() == ScalarAttributeType.N

			// Check secondary indexes
			description.globalSecondaryIndexes().size() == 2
			
			// Check custom named index
			def catPriceIndex = description.globalSecondaryIndexes().find { it.indexName() == 'cat-price-index' }
			catPriceIndex != null
			catPriceIndex.keySchema().size() == 2
			catPriceIndex.keySchema()[0].attributeName() == 'category'
			catPriceIndex.keySchema()[0].keyType() == KeyType.HASH
			catPriceIndex.keySchema()[1].attributeName() == 'price'
			catPriceIndex.keySchema()[1].keyType() == KeyType.RANGE
			
			// Check auto-named index
			def statusIndex = description.globalSecondaryIndexes().find { it.indexName() == 'status-count-index' }
			statusIndex != null
			statusIndex.keySchema().size() == 2
			statusIndex.keySchema()[0].attributeName() == 'status'
			statusIndex.keySchema()[0].keyType() == KeyType.HASH
			statusIndex.keySchema()[1].attributeName() == 'count'
			statusIndex.keySchema()[1].keyType() == KeyType.RANGE

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should verify table functionality after creation with different key types"() { // {{{
		given:
			String table = 'test_table_functionality'
			Scalar partKey = Scalar.of('id', ScalarAttributeType.S)
			Scalar sortKey = Scalar.of('sortKey', ScalarAttributeType.S)
			Index[] indexes = [
				Index.of(
					Scalar.of('email', ScalarAttributeType.S)
				),
				Index.of(
					Scalar.of('status', ScalarAttributeType.S),
					Scalar.of('timestamp', ScalarAttributeType.N)
				)
			]

		when:
			dynamo.createTable (
				table,
				partKey,
				Optional.of(sortKey),
				indexes
			)

		and: 'Insert test items'
			List<TestItem> items = [
				new TestItem (
					id: 'user1',
					sortKey: 'record1',
					email: 'user1@example.com',
					status: 'active',
					timestamp: 1000
				),
				new TestItem (
					id: 'user2',
					sortKey: 'record1',
					email: 'user2@example.com',
					status: 'inactive',
					timestamp: 2000
				),
				new TestItem (
					id: 'user1',
					sortKey: 'record2',
					email: 'user1@example.com',
					status: 'active',
					timestamp: 3000
				)
			]
			dynamo.putItems(table, items)

		then: 'Can query by primary key'
			TestItem item = dynamo.getItem (
				table,
				KeyFilter.of('id', 'user1', 'sortKey', 'record1'),
				TestItem
			)
			item != null
			item.id == 'user1'
			item.sortKey == 'record1'
			item.email == 'user1@example.com'

		and: 'Can query using the email index'
			List<TestItem> userItems = dynamo.query (
				table, 
				'email-index',
				KeyFilter.of('email', 'user1@example.com'),
				TestItem
			)
			.list()
			userItems.size() == 2
			userItems.every { it.email == 'user1@example.com' }
			userItems.every { it.id == 'user1' }
			userItems.collect { it.sortKey }.sort() == ['record1', 'record2']

		and: 'Can query using the status-timestamp index'
			List<TestItem> activeItems = dynamo.query (
				table,
				'status-timestamp-index',
				KeyFilter.of('status', 'active'),
				TestItem
			)
			.list()
		and:
			activeItems.size() == 2
			activeItems.every { it.status == 'active' }
			activeItems.find { it.timestamp == 1000 && it.id == 'user1' && it.sortKey == 'record1' }
			activeItems.find { it.timestamp == 3000 && it.id == 'user1' && it.sortKey == 'record2' }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	@Ignore
	// Both these options are ignored in the local version of DynamoDB
	// see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html
	def "Should perform parallel and limited scan operations"() { // {{{
		given:
			String table = 'test_less_common_scan'
			String partKey = 'id'
		and:
			dynamo.createTable (
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
			dynamo.putItems(table, items)

		when: 'Performing a parallel scan with 2 segments'
			List<TestItem> segment0Results = dynamo
				.scan(table, TestItem)
				.segment(0, 2)
				.list()

			List<TestItem> segment1Results = dynamo
				.scan(table, TestItem)
				.segment(1, 2)
				.list()

		then: 'The combined results should contain all items'
			def combinedResults = segment0Results + segment1Results
			combinedResults.size() == items.size()

			// Check that the segments don't overlap and contain all items
			def segmentIds = segment0Results*.id + segment1Results*.id
			segmentIds.sort() == items*.id.sort()

		when: 'Performing a filtered parallel scan'
			DynamoFilter enabledFilter = match('enabled', true)

			List<TestItem> filteredSegment0 = dynamo
				.scan(table, TestItem)
				.filter(enabledFilter)
				.segment(0, 2)
				.list()

			List<TestItem> filteredSegment1 = dynamo
				.scan(table, TestItem)
				.filter(enabledFilter)
				.segment(1, 2)
				.list()

		then: 'The combined filtered results should contain only enabled items'
			def combinedFiltered = filteredSegment0 + filteredSegment1
			combinedFiltered.every { it.enabled }
			combinedFiltered.size() == items.count { it.enabled }

		when: 'scanning with a limit'
			List<TestItem> limitedResults = dynamo
				.scan(table, TestItem)
				.limit(2)
				.list()

		then: 'only the specified number of items should be returned'
			limitedResults.size() <= 2

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should remove attributes from existing items"() { // {{{
		given:
			String table = 'test_remove_attributes'
			String partKey = 'id'
		and:
			dynamo.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'test_item',
				data: 'some data',
				tagField: 'tag_value',
				enabled: true
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamo.putItem(table, item)

		expect: 'Item has all initial attributes'
			TestItem initial = dynamo.getItem (
				table, key, TestItem
			)
			initial != null
			initial.data == 'some data'
			initial.tagField == 'tag_value'
			initial.enabled == true

		when: 'Remove some attributes'
			dynamo.removeAttributes (
				table,
				key,
				'data', 'tagField'
			)

		then: 'Specified attributes should be removed'
			TestItem updated = dynamo.getItem (
				table, key, TestItem
			)
			updated != null
			updated.id == 'test_item'
			updated.data == null
			updated.tagField == null
			updated.enabled == true  // This attribute should remain
			updated.version == 1     // Version should remain

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle removing non-existent attributes gracefully"() { // {{{
		given:
			String table = 'test_remove_nonexistent'
			String partKey = 'id'
		and:
			dynamo.createTable (
				table,
				partKey
			)
		and:
			TestItem item = new TestItem (
				id: 'test_item',
				data: 'some data'
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamo.putItem(table, item)

		when: 'Remove non-existent attributes'
			dynamo.removeAttributes (
				table,
				key,
				'nonExistentField1', 'nonExistentField2'
			)

		then: 'Operation should succeed without errors'
			noExceptionThrown()

		and: 'Existing data should remain unchanged'
			TestItem unchanged = dynamo.getItem (
				table, key, TestItem
			)
			unchanged != null
			unchanged.id == 'test_item'
			unchanged.data == 'some data'
			unchanged.version == 1

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should remove attributes from items with composite keys"() { // {{{
		given:
			String table = 'test_remove_composite'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable (
				table,
				partKey,
				sortKey
			)
		and:
			TestItem item = new TestItem (
				id: 'parent1',
				sortKey: 'child1',
				data: 'original data',
				tagField: 'original tag',
				enabled: true
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamo.putItem(table, item)

		when: 'Remove attributes from composite key item'
			dynamo.removeAttributes (
				table,
				key,
				'data', 'enabled'
			)

		then: 'Specified attributes should be removed'
			TestItem updated = dynamo.getItem (
				table, key, TestItem
			)
			updated != null
			updated.id == 'parent1'
			updated.sortKey == 'child1'
			updated.data == null
			updated.enabled == false  // boolean fields default to false when null
			updated.tagField == 'original tag'  // This should remain
			updated.version == 1

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should prevent removing key attributes"() { // {{{
		given:
			String table = 'test_remove_key_protection'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable (
				table,
				partKey,
				sortKey
			)
		and:
			TestItem item = new TestItem (
				id: 'test_id',
				sortKey: 'test_sort',
				data: 'test data'
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamo.putItem(table, item)

		when: 'Try to remove key attributes'
			dynamo.removeAttributes (
				table,
				key,
				'id', 'sortKey'  // Key attributes only
			)

		then: 'DynamoDB should throw an exception'
			def exception = thrown(DynamoDbException)
			exception.message.contains('Cannot update attribute')
			exception.message.contains('This attribute is part of the key')

		and: 'Item should remain unchanged'
			TestItem unchanged = dynamo.getItem (
				table, key, TestItem
			)
			unchanged != null
			unchanged.id == 'test_id'
			unchanged.sortKey == 'test_sort'
			unchanged.data == 'test data'

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with sort key range conditions using greater than"() { // {{{
		given:
			String table = 'test_sort_range_gt'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'user1', sortKey: '1000', timestamp: 1000),
				new TestItem(id: 'user1', sortKey: '2000', timestamp: 2000),
				new TestItem(id: 'user1', sortKey: '3000', timestamp: 3000),
				new TestItem(id: 'user1', sortKey: '4000', timestamp: 4000),
				new TestItem(id: 'user2', sortKey: '1500', timestamp: 1500)
			]
		and:
			dynamo.putItems(table, items)

		when: 'Query with timestamp > 2000'
			KeyFilter rangeKey = KeyFilter.of (
				'id', 'user1',
				greater('sortKey', '2000')
			)
		and:
			List<TestItem> results = dynamo
				.query(table, rangeKey, TestItem)
				.list()

		then:
			results.size() == 2
			results.every { it.id == 'user1' }
			results.every { it.timestamp > 2000 }
			results*.timestamp.sort() == [3000, 4000]

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with sort key range conditions using between"() { // {{{
		given:
			String table = 'test_sort_range_between'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'game1', sortKey: '50', data: 'score50'),
				new TestItem(id: 'game1', sortKey: '150', data: 'score150'),
				new TestItem(id: 'game1', sortKey: '250', data: 'score250'),
				new TestItem(id: 'game1', sortKey: '350', data: 'score350'),
				new TestItem(id: 'game1', sortKey: '450', data: 'score450')
			]
			dynamo.putItems(table, items)

		when: 'Query with score BETWEEN 100 AND 300'
			KeyFilter rangeKey = KeyFilter.of(
				'id', 'game1',
				between('sortKey', '100', '300')
			)
			List<TestItem> results = dynamo
				.query(table, rangeKey, TestItem)
				.list()

		then:
			results.size() == 2
			results.every { it.id == 'game1' }
			results*.data.sort() == ['score150', 'score250']

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with sort key begins_with condition"() { // {{{
		given:
			String table = 'test_sort_begins_with'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'user1', sortKey: 'ORDER_CREATED', data: 'order1'),
				new TestItem(id: 'user1', sortKey: 'ORDER_CANCELLED', data: 'order2'),
				new TestItem(id: 'user1', sortKey: 'LOGIN_SUCCESS', data: 'login1'),
				new TestItem(id: 'user1', sortKey: 'ORDER_SHIPPED', data: 'order3'),
				new TestItem(id: 'user1', sortKey: 'LOGOUT', data: 'logout1')
			]
			dynamo.putItems(table, items)

		when: 'Query with eventType begins_with "ORDER"'
			KeyFilter rangeKey = KeyFilter.of(
				'id', 'user1',
				beginsWith('sortKey', 'ORDER')
			)
			List<TestItem> results = dynamo
				.query(table, rangeKey, TestItem)
				.list()

		then:
			results.size() == 3
			results.every { it.id == 'user1' }
			results.every { it.sortKey.startsWith('ORDER') }
			results*.data.sort() == ['order1', 'order2', 'order3']

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with sort key range conditions and additional filters"() { // {{{
		given:
			String table = 'test_sort_range_with_filter'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'user1', sortKey: '1000', timestamp: 1000, status: 'ACTIVE', enabled: true),
				new TestItem(id: 'user1', sortKey: '2000', timestamp: 2000, status: 'ACTIVE', enabled: false),
				new TestItem(id: 'user1', sortKey: '3000', timestamp: 3000, status: 'INACTIVE', enabled: true),
				new TestItem(id: 'user1', sortKey: '4000', timestamp: 4000, status: 'ACTIVE', enabled: true),
				new TestItem(id: 'user1', sortKey: '5000', timestamp: 5000, status: 'ACTIVE', enabled: false)
			]
			dynamo.putItems(table, items)

		when: 'Query with timestamp >= 2000 AND status = ACTIVE AND enabled = true'
			KeyFilter rangeKey = KeyFilter.of(
				'id', 'user1',
				greaterOrEqual('sortKey', '2000')
			)
			DynamoFilter additionalFilter = every(
				match('status', 'ACTIVE'),
				match('enabled', true)
			)
			List<TestItem> results = dynamo
				.query(table, rangeKey, TestItem)
				.filter(additionalFilter)
				.list()

		then:
			results.size() == 1
			results.first().id == 'user1'
			results.first().timestamp == 4000
			results.first().status == 'ACTIVE'
			results.first().enabled == true

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with complex sort key conditions using AND/OR"() { // {{{
		given:
			String table = 'test_sort_complex_conditions'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'store1', sortKey: 'ELECTRONICS', data: 'laptop', enabled: true),
				new TestItem(id: 'store1', sortKey: 'BOOKS', data: 'novel', enabled: false),
				new TestItem(id: 'store1', sortKey: 'CLOTHING', data: 'shirt', enabled: true),
				new TestItem(id: 'store1', sortKey: 'ELECTRONICS2', data: 'phone', enabled: false),
				new TestItem(id: 'store1', sortKey: 'FOOD', data: 'apple', enabled: true)
			]
			dynamo.putItems(table, items)

		when: 'Query with (category = ELECTRONICS AND enabled = true) OR (category = CLOTHING)'
			DynamoFilter complexFilter = every(
				match('id', 'store1'),
				any(
					every(match('sortKey', 'ELECTRONICS'), match('enabled', true)),
					match('sortKey', 'CLOTHING')
				)
			)
			List<TestItem> results = dynamo
				.scan(table, TestItem)
				.filter(complexFilter)
				.list()

		then:
			results.size() == 2
			results.every { it.id == 'store1' }
			def resultData = results*.data.sort()
			resultData == ['laptop', 'shirt']

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with sort key range conditions on secondary index"() { // {{{
		given:
			String table = 'test_sort_range_index'
			String partKey = 'id'
			String sortKey = 'sortKey'
			Index scoreIndex = Index.of('status', 'data', 'status-score-index')
		and:
			dynamo.createTable(
				table,
				Scalar.of(partKey),
				Optional.of(Scalar.of(sortKey)),
				scoreIndex
			)
		and:
			def items = [
				new TestItem(id: 'game1', sortKey: '1000', status: 'ACTIVE', timestamp: 1000, data: '100'),
				new TestItem(id: 'game1', sortKey: '2000', status: 'ACTIVE', timestamp: 2000, data: '200'),
				new TestItem(id: 'game1', sortKey: '3000', status: 'ACTIVE', timestamp: 3000, data: '300'),
				new TestItem(id: 'game2', sortKey: '1500', status: 'ACTIVE', timestamp: 1500, data: '150'),
				new TestItem(id: 'game3', sortKey: '2500', status: 'INACTIVE', timestamp: 2500, data: '250')
			]
			dynamo.putItems(table, items)

		when: 'Query index with status = ACTIVE AND score > 150'
			KeyFilter indexKey = KeyFilter.of(
				'status', 'ACTIVE',
				greater('data', '150')
			)
			List<TestItem> results = dynamo.query (
				table,
				'status-score-index',
				indexKey,
				TestItem
			)
			.list()

		then:
			results.size() == 2
			results.every { it.status == 'ACTIVE' }
			results*.data.sort() == ['200', '300']
			results*.id.sort() == ['game1', 'game1']

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle sort key less than and less than or equal conditions"() { // {{{
		given:
			String table = 'test_sort_less_than'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'tasks', sortKey: '1', data: 'urgent'),
				new TestItem(id: 'tasks', sortKey: '3', data: 'high'),
				new TestItem(id: 'tasks', sortKey: '5', data: 'medium'),
				new TestItem(id: 'tasks', sortKey: '7', data: 'low'),
				new TestItem(id: 'tasks', sortKey: '9', data: 'lowest')
			]
			dynamo.putItems(table, items)

		when: 'Query with priority < 5'
			KeyFilter lessThanKey = KeyFilter.of(
				'id', 'tasks',
				less('sortKey', '5')
			)
			List<TestItem> lessThanResults = dynamo
				.query(table, lessThanKey, TestItem)
				.list()

		then:
			lessThanResults.size() == 2
			lessThanResults*.data.sort() == ['high', 'urgent']

		when: 'Query with priority <= 5'
			KeyFilter lessOrEqualKey = KeyFilter.of(
				'id', 'tasks',
				lessOrEqual('sortKey', '5')
			)
			List<TestItem> lessOrEqualResults = dynamo
				.query(table, lessOrEqualKey, TestItem)
				.list()

		then:
			lessOrEqualResults.size() == 3
			lessOrEqualResults*.data.sort() == ['high', 'medium', 'urgent']

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should maintain backward compatibility with exact sort key matching"() { // {{{
		given:
			String table = 'test_backward_compatibility'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'user1', sortKey: '1000', timestamp: 1000, data: 'first'),
				new TestItem(id: 'user1', sortKey: '2000', timestamp: 2000, data: 'second'),
				new TestItem(id: 'user1', sortKey: '3000', timestamp: 3000, data: 'third')
			]
			dynamo.putItems(table, items)

		when: 'Traditional exact matching still works'
			KeyFilter exactKey = KeyFilter.of('id', 'user1', 'sortKey', '2000')
			TestItem exactResult = dynamo.getItem(
				table,
				exactKey,
				TestItem
			)

		then:
			exactResult != null
			exactResult.data == 'second'
			exactResult.timestamp == 2000

		when: 'Range-based exact matching (functionally equivalent)'
			KeyFilter rangeExactKey = KeyFilter.of(
				'id', 'user1',
				match('sortKey', '2000')
			)
			List<TestItem> rangeExactResults = dynamo
				.query(table, rangeExactKey, TestItem)
				.list()

		then:
			rangeExactResults.size() == 1
			rangeExactResults.first().data == 'second'
			rangeExactResults.first().timestamp == 2000

		and: 'Both approaches produce equivalent results'
			exactResult.id == rangeExactResults.first().id
			exactResult.sortKey == rangeExactResults.first().sortKey
			exactResult.data == rangeExactResults.first().data

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with numeric sort key ranges"() { // {{{
		given:
			String table = 'test_numeric_sort_range'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			def items = [
				new TestItem(id: 'game1', sortKey: '85', timestamp: 85, data: 'player1'),
				new TestItem(id: 'game1', sortKey: '92', timestamp: 92, data: 'player2'),
				new TestItem(id: 'game1', sortKey: '78', timestamp: 78, data: 'player3'),
				new TestItem(id: 'game1', sortKey: '96', timestamp: 96, data: 'player4'),
				new TestItem(id: 'game1', sortKey: '88', timestamp: 88, data: 'player5')
			]
			dynamo.putItems(table, items)

		when: 'Query with score between 80 and 95'
			KeyFilter numericRangeKey = KeyFilter.of(
				'id', 'game1',
				between('sortKey', '80', '95')
			)
			List<TestItem> results = dynamo
				.query(table, numericRangeKey, TestItem)
				.list()

		then:
			results.size() == 3
			results.every { it.id == 'game1' }
			def scores = results*.timestamp.sort()
			scores == [85, 88, 92]
			results*.data.sort() == ['player1', 'player2', 'player5']

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should automatically retrieve all results with query method using pagination"() { // {{{
		given:
			String table = 'test_auto_pagination'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and: 'Create enough items to require multiple pages'
			List<TestItem> items = (1..15).collect { int i ->
				new TestItem(
					id: 'user1',
					sortKey: String.format('%03d', i),
					data: "data${i}"
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query method (which should automatically paginate)'
			List<TestItem> allResults = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.list()

		then: 'All items should be retrieved'
			allResults.size() == 15
			allResults.every { it.id == 'user1' }
			allResults*.sortKey.sort() == (1..15).collect { String.format('%03d', it) }

		when: 'Use query method for manual pagination control'
			PagedResult<TestItem> firstPage = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.paged(5)

		then: 'Only first page should be returned'
			firstPage.items.size() == 5
			firstPage.more == true
			firstPage.last != null

		when: 'Get second page'
			PagedResult<TestItem> secondPage = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.paged(5, firstPage.last, true)

		then: 'Second page should have items and continue pagination'
			secondPage.items.size() == 5
			secondPage.more == true
			secondPage.last != null

		when: 'Get remaining items'
			PagedResult<TestItem> thirdPage = dynamo
				.query(table, KeyFilter.of('id', 'user1'), TestItem)
				.paged(10, secondPage.last, true)

		then: 'Last page should have remaining items'
			thirdPage.items.size() == 5
			thirdPage.more == false
			thirdPage.last == null || thirdPage.last.isEmpty()

		and: 'Combined pages should equal auto-paginated results'
			def manualResults = firstPage.items + secondPage.items + thirdPage.items
			manualResults.size() == allResults.size()
			manualResults*.sortKey.sort() == allResults*.sortKey.sort()

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should automatically retrieve all results with query using index and pagination"() { // {{{
		given:
			String table = 'test_auto_pagination_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'status_index': 'status'
			]
		and:
			dynamo.createTable(table, partKey, null, indexes)
		and: 'Create enough items to require multiple pages'
			List<TestItem> items = (1..12).collect { int i ->
				new TestItem(
					id: "item${i}",
					status: 'ACTIVE',
					data: "data${i}"
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query method with index (should automatically paginate)'
			List<TestItem> allResults = dynamo.query (
				table,
				'status_index',
				KeyFilter.of('status', 'ACTIVE'),
				TestItem
			)
			.list()

		then: 'All items should be retrieved'
			allResults.size() == 12
			allResults.every { it.status == 'ACTIVE' }

		when: 'Use query method with index for manual pagination'
			PagedResult<TestItem> pagedResult = dynamo.query (
				table,
				'status_index',
				KeyFilter.of('status', 'ACTIVE'),
				TestItem
			)
			.paged(5)

		then: 'Only limited items should be returned'
			pagedResult.items.size() == 5
			pagedResult.more == true
			pagedResult.last != null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should automatically retrieve all results with filters and pagination"() { // {{{
		given:
			String table = 'test_auto_pagination_filter'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and: 'Create items with mixed enabled status'
			List<TestItem> items = (1..20).collect { int i ->
				new TestItem (
					id: 'user1',
					sortKey: String.format('%03d', i),
					data: "data${i}",
					enabled: (i % 2 == 0)  // every other item enabled
				)
			}
			dynamo.putItems(table, items)

		when: 'Use query method with filter (should automatically paginate)'
			List<TestItem> enabledResults = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			)
			.filter(match('enabled', true))
			.list()

		then: 'Only enabled items should be retrieved'
			enabledResults.size() == 10  // half the items
			enabledResults.every { it.enabled == true }
			enabledResults.every { it.id == 'user1' }

		when: 'Use query method with filter for manual pagination'
			PagedResult<TestItem> pagedFiltered = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			)
			.filter(match('enabled', true))
			.paged(3)

		then: 'Only limited filtered items should be returned'
			pagedFiltered.items.size() <= 3
			pagedFiltered.items.every { it.enabled == true }

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should retrieve item with projection to select specific fields"() { // {{{
		given:
			String table = 'test_projection_get'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			TestItem item = new TestItem (
				id: 'proj1',
				data: 'sensitive data',
				tagField: 'public tag',
				enabled: true
			)
		and:
			KeyFilter key = KeyFilter.of (
				item.impress(new DynamoMapper()).key()
			)
		and:
			dynamo.putItem(table, item)

		when: 'Retrieve item with projection for specific fields'
			DynamoMap projectedItem = dynamo.getItem (
				table,
				key,
				['id', 'tagField'],
				DynamoMap
			)

		then: 'Only projected fields should be returned'
			projectedItem != null
			projectedItem.id == 'proj1'
			projectedItem.tagField == 'public tag'
			projectedItem.data == null
			projectedItem.enabled == null   // not projected, so null
			projectedItem.version == null   // not projected

		when: 'Retrieve item without projection'
			TestItem fullItem = dynamo.getItem (
				table,
				key,
				TestItem
			)

		then: 'All fields should be returned'
			fullItem != null
			fullItem.id == 'proj1'
			fullItem.data == 'sensitive data'
			fullItem.tagField == 'public tag'
			fullItem.enabled == true
			fullItem.version == 1

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with projection to select specific fields"() { // {{{
		given:
			String table = 'test_projection_query'
			String partKey = 'id'
			String sortKey = 'sortKey'
		and:
			dynamo.createTable(table, partKey, sortKey)
		and:
			List<TestItem> items = [
				new TestItem (
					id: 'user1',
					sortKey: 'rec1',
					data: 'secret info',
					tagField: 'public',
					enabled: true
				),
				new TestItem (
					id: 'user1',
					sortKey: 'rec2',
					data: 'confidential',
					tagField: 'open',
					enabled: false
				)
			]
		and:
			dynamo.putItems(table, items)

		when: 'Query with projection for specific fields'
			List<DynamoMap> projectedItems = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				DynamoMap
			)
			.fields('id', 'sortKey', 'tagField')
			.list()

		then: 'Only projected fields should be returned'
			projectedItems.size() == 2
			projectedItems.every { item ->
				item.id == 'user1' &&
				item.sortKey in ['rec1', 'rec2'] &&
				item.tagField in ['public', 'open'] &&
				item.data == null &&
				item.enabled == null &&
				item.version == null
			}

		when: 'Query without projection'
			List<TestItem> fullItems = dynamo.query (
				table,
				KeyFilter.of('id', 'user1'),
				TestItem
			).list()

		then: 'All fields should be returned'
			fullItems.size() == 2
			fullItems.every { item ->
				item.id == 'user1' &&
				item.sortKey in ['rec1', 'rec2'] &&
				item.data in ['secret info', 'confidential'] &&
				item.tagField in ['public', 'open'] &&
				item.version == 1
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should query with projection using index"() { // {{{
		given:
			String table = 'test_projection_query_index'
			String partKey = 'id'
			Map<String, String> indexes = [
				'tag_index': 'tagField'
			]
		and:
			dynamo.createTable(table, partKey, null, indexes)
		and:
			List<TestItem> items = [
				new TestItem (
					id: 'item1',
					tagField: 'category_a',
					data: 'private data',
					enabled: true
				),
				new TestItem (
					id: 'item2',
					tagField: 'category_a',
					data: 'more private',
					enabled: false
				)
			]
		and:
			dynamo.putItems(table, items)

		when: 'Query index with projection'
			List<DynamoMap> projectedResults = dynamo.query (
				table,
				'tag_index',
				KeyFilter.of('tagField', 'category_a')
			)
			.fields(['id', 'tagField'])
			.list()

		then: 'Only projected fields should be returned'
			projectedResults.size() == 2
			projectedResults.every { item ->
				item.tagField == 'category_a' &&
				item.id in ['item1', 'item2'] &&
				item.data == null &&
				item.enabled == null &&
				item.version == null
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with projection to select specific fields"() { // {{{
		given:
			String table = 'test_scan_projection'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem (
					id: 'scan_proj1',
					data: 'sensitive info',
					tagField: 'public_tag',
					enabled: true
				),
				new TestItem (
					id: 'scan_proj2',
					data: 'confidential',
					tagField: 'open_tag',
					enabled: false
				),
				new TestItem (
					id: 'scan_proj3',
					data: 'secret',
					tagField: 'visible_tag',
					enabled: true
				)
			]
		and:
			dynamo.putItems(table, items)

		when: 'Scan with projection for specific fields'
			List<DynamoMap> projectedItems = dynamo
				.scan(table)
				.fields(['id', 'tagField'])
				.list()

		then: 'Only projected fields should be returned'
			projectedItems.size() == 3
			projectedItems.every { item ->
				item.id in ['scan_proj1', 'scan_proj2', 'scan_proj3'] &&
				item.tagField in ['public_tag', 'open_tag', 'visible_tag'] &&
				item.data == null &&
				item.enabled == null &&
				item.version == null
			}

		when: 'Scan without projection'
			List<TestItem> fullItems = dynamo.scan (
				table, TestItem
			).list()

		then: 'All fields should be returned'
			fullItems.size() == 3
			fullItems.every { item ->
				item.id in ['scan_proj1', 'scan_proj2', 'scan_proj3'] &&
				item.data in ['sensitive info', 'confidential', 'secret'] &&
				item.tagField in ['public_tag', 'open_tag', 'visible_tag'] &&
				item.version == 1
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with filter and projection combined"() { // {{{
		given:
			String table = 'test_scan_filter_projection'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem (
					id: 'user1',
					data: 'private data 1',
					tagField: 'category_a',
					enabled: true
				),
				new TestItem (
					id: 'user2',
					data: 'private data 2',
					tagField: 'category_a',
					enabled: false
				),
				new TestItem (
					id: 'user3',
					data: 'private data 3',
					tagField: 'category_b',
					enabled: true
				)
			]
		and:
			dynamo.putItems(table, items)

		when: 'Scan with filter and projection'
			List<DynamoMap> filteredProjectedItems = dynamo
				.scan(table)
				.filter(every (
					match('tagField', 'category_a'),
					match('enabled', true)
				))
				.fields(['id', 'tagField', 'enabled'])
				.list()

		then: 'Only filtered items with projected fields should be returned'
			filteredProjectedItems.size() == 1
			def item = filteredProjectedItems.first()
			item.id == 'user1'
			item.tagField == 'category_a'
			item.enabled == true
			item.data == null
			item.version == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with simplified interface - table name and projection only"() { // {{{
		given:
			String table = 'test_scan_simple'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			TestItem item = new TestItem (
				id: 'simple1',
				data: 'some data',
				tagField: 'tag_value',
				enabled: true
			)
		and:
			dynamo.putItem(table, item)

		when: 'Use simplest scan interface'
			List<DynamoMap> allItems = dynamo
				.scan(table)
				.list()

		then: 'All items with all fields should be returned'
			allItems.size() == 1
			allItems.first().id == 'simple1'
			allItems.first().data == 'some data'
			allItems.first().tagField == 'tag_value'
			allItems.first().enabled == true

		when: 'Use scan with only projection'
			List<DynamoMap> projectedOnly = dynamo
				.scan(table)
				.fields(['id', 'tagField'])
				.list()

		then: 'Only projected fields should be returned'
			projectedOnly.size() == 1
			def projItem = projectedOnly.first()
			projItem.id == 'simple1'
			projItem.tagField == 'tag_value'
			projItem.data == null
			projItem.enabled == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with projection using full signature including limit and segments"() { // {{{
		given:
			String table = 'test_scan_full_signature'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = (1..10).collect { int i ->
				new TestItem (
					id: "full_sig${i}",
					data: "data${i}",
					tagField: "tag${i}",
					enabled: (i % 2 == 0)
				)
			}
		and:
			dynamo.putItems(table, items)

		when: 'Use full signature with projection and limit'
			List<DynamoMap> limitedProjected = dynamo
				.scan(table)
				.filter(match('enabled', true))
				.fields(['id', 'enabled'])
				.limit(3)
				.list()

		then: 'Projected results with filter should be returned'
			limitedProjected.every { item ->
				item.enabled == true &&
				item.id != null &&
				item.data == null &&
				item.tagField == null &&
				item.version == null
			}
			// Note: DynamoDB limit controls items examined, not returned.
			// With filtering, the number of returned items may vary.

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should maintain backward compatibility for existing scan calls"() { // {{{
		given:
			String table = 'test_scan_backward_compat'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			List<TestItem> items = [
				new TestItem(id: 'compat1', data: 'data1', enabled: true),
				new TestItem(id: 'compat2', data: 'data2', enabled: false)
			]
		and:
			dynamo.putItems(table, items)

		when: 'Use old scan signature - scan(table, filter, type)'
			List<TestItem> oldSigResults = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.list()

		then: 'Should work as before'
			oldSigResults.size() == 1
			oldSigResults.first().id == 'compat1'
			oldSigResults.first().data == 'data1'
			oldSigResults.first().enabled == true
			oldSigResults.first().version == 1

		when: 'Use old scan signature - scan(table, filter, type, limit)'
			List<TestItem> limitedResults = dynamo
				.scan(table, TestItem)
				.limit(1)
				.list()

		then: 'Should work as before'
			limitedResults.every { it.version == 1 }
			// Note: DynamoDB limit controls items examined, not returned

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should return PagedResult when scan pagination parameters are used"() { // {{{
		given:
			String table = 'test_scan_paged'
			dynamo.createTable(table, 'id')
			Integer totalSize = 15
			Integer pageSize = 5
		and:
			List<TestItem> items = (1..totalSize).collect {
				new TestItem(id: "scan_item${it}", data: "data${it}")
			}
			dynamo.putItems(table, items)

		when: 'Using limit parameter for scan'
			PagedResult<TestItem> first = dynamo
				.scan(table, TestItem)
				.paged(pageSize, null)

		then:
			first.count <= pageSize
			first.more == true
			first.last != null

		when: 'Using last parameter for next page'
			PagedResult<TestItem> second = dynamo
				.scan(table, TestItem)
				.paged(pageSize, first.last)

		then:
			second.count <= pageSize
			second.last != null

		when: 'Get remaining items'
			PagedResult<TestItem> third = dynamo
				.scan(table, TestItem)
				.paged(totalSize, second.last)

		then:
			third.more == false

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with PagedResult and filter"() { // {{{
		given:
			String table = 'test_scan_paged_filter'
			dynamo.createTable(table, 'id')
		and:
			List<TestItem> items = (1..10).collect {
				new TestItem(id: "filter_item${it}", enabled: (it % 2 == 0))
			}
			dynamo.putItems(table, items)

		when: 'Scan with filter and pagination'
			PagedResult<TestItem> filtered = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.paged(3, null)

		then:
			filtered.items.every { it.enabled == true }
			filtered.count <= 3

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with PagedResult, filter and projection"() { // {{{
		given:
			String table = 'test_scan_paged_filter_projection'
			dynamo.createTable(table, 'id')
		and:
			List<TestItem> items = (1..8).collect {
				new TestItem(
					id: "proj_item${it}", 
					data: "secret${it}",
					tagField: "public${it}",
					enabled: (it % 2 == 0)
				)
			}
			dynamo.putItems(table, items)

		when: 'Scan with filter, projection and pagination'
			PagedResult<DynamoMap> result = dynamo
				.scan(table)
				.filter(match('enabled', true))
				.fields(['id', 'tagField'])
				.paged(2, null)

		then:
			result.items.every { item ->
				item.id != null &&
				item.tagField != null &&
				item.data == null &&
				item.enabled == null
			}
			result.count <= 2

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should scan with PagedResult using various convenience method signatures"() { // {{{
		given:
			String table = 'test_scan_convenience'
			dynamo.createTable(table, 'id')
		and:
			List<TestItem> items = (1..6).collect {
				new TestItem(
					id: "conv_item${it}", 
					data: "secret${it}",
					tagField: "public${it}",
					enabled: (it % 2 == 0)
				)
			}
			dynamo.putItems(table, items)

		when: 'Simple scan with limit only'
			PagedResult<TestItem> simple = dynamo
				.scan(table, TestItem)
				.paged(3, null)

		then:
			simple.count <= 3
			simple.items.every { it instanceof TestItem }

		when: 'Scan with filter and limit'
			PagedResult<TestItem> filtered = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.paged(2, null)

		then:
			filtered.items.every { it.enabled == true }
			filtered.count <= 2

		when: 'Scan with projection, filter and limit'
			PagedResult<DynamoMap> projected = dynamo
				.scan(table)
				.filter(match('enabled', true))
				.fields(['id', 'tagField'])
				.paged(2, null)

		then:
			projected.items.every { item ->
				item.id != null &&
				item.tagField != null &&
				item.data == null &&
				item.enabled == null
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should auto-extract keys for getItem operation"() { // {{{
		given:
			String table = 'auto_key_get_test'
			String partKey = 'id'
		and:
			dynamo.createTable (
				table,
				partKey
			)
		and:
			DynamoMap original = new DynamoMap (
				id: 'test123',
				payload: 'original',
				v: 1
			)

		when: 'put item using traditional method'
			dynamo.putItem(table, original)
		and:
			dynamo.updateItem (
				table,
				original.tap { payload = 'updated'; v = 2 }
			)

		then: 'auto-extract keys and retrieve the item'
			DynamoMap refreshed = dynamo.refreshItem (
				table,
				original
			)
		and:
			verifyAll(refreshed) {
				id == 'test123'
				payload == 'updated'
				v == 2
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should auto-extract composite keys for getItem operation"() { // {{{
		given:
			String table = 'auto_key_composite_test'
		and:
			dynamo.createTable (
				table,
				'contract',
				'sheet'
			)
		and:
			DynamoMap original = new DynamoMap (
				contract: 'contract123',
				sheet: 'sheet456',
				offer: 'special offer',
				payload: 'important data'
			)

		when: 'put item using traditional method'
			dynamo.putItem(table, original)
		and:
			dynamo.updateItem (
				table,
				original.tap { payload = 'updated' }
			)

		then: 'auto-extract keys and retrieve the item'
			DynamoMap refreshed = dynamo.refreshItem (
				table,
				original
			)
		and:
			verifyAll(refreshed) {
				contract == 'contract123'
				sheet == 'sheet456'
				offer == 'special offer'
				payload == 'updated'
			}

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should auto-extract keys for deleteItem operation"() { // {{{
		given:
			String table = 'auto_key_delete_test'
			KeyFilter key = KeyFilter.of('id', 'delete_me')
		and:
			dynamo.createTable (
				table,
				'id'
			)
		and:
			DynamoMap item = new DynamoMap (
				id: 'delete_me',
				payload: 'to be deleted'
			)

		when: 'put item'
			dynamo.putItem(table, item)

		then: 'verify item exists'
			dynamo.getItem(table, key) != null

		when: 'delete using auto-key-extraction'
			dynamo.deleteItem(table, item)

		then: 'verify item is deleted'
			dynamo.getItem(table, key) == null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle missing key attributes gracefully"() { // {{{
		given:
			String table = 'missing_key_test'
		and:
			dynamo.createTable (
				table,
				'id'
			)
		and:
			DynamoMap item = new DynamoMap (
				field: 'no key data'
			)

		when: 'try to extract keys from item without key data'
			dynamo.extractKey(table, item, false)

		then: 'should throw appropriate exception'
			thrown(IllegalStateException)

		cleanup:
			dynamo.dropTable(table)
	} // }}}


	static class TestItem // {{{
		implements Storable<AttributeValue,String> {
		String id
		String sortKey
		String email
		String status
		BigInteger timestamp
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

			if (email)
				mapper.with('email', email)

			if (status)
				mapper.with('status', status)

			if (timestamp)
				mapper.with('timestamp', timestamp)

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

	def "Should support fluent builder pattern for queries and scans"() { // {{{
		given:
			String table = 'test_builder_pattern'
			dynamo.createTable(table, 'id', 'sortKey')
		and:
			List<TestItem> items = [
				new TestItem(id: 'user1', sortKey: '2025-01-01', data: 'data1', enabled: true),
				new TestItem(id: 'user1', sortKey: '2025-01-02', data: 'data2', enabled: false),
				new TestItem(id: 'user1', sortKey: '2025-01-03', data: 'data3', enabled: true),
				new TestItem(id: 'user2', sortKey: '2025-01-01', data: 'data4', enabled: true)
			]
			dynamo.putItems(table, items)

		when: 'Using the fluent query builder'
			List<TestItem> builderResults = dynamo
				.query (
					table,
					KeyFilter.of('id', 'user1'),
					TestItem
				)
				.backward()
				.list()

		then: 'Results should be in reverse order'
			builderResults.size() == 3
			builderResults[0].sortKey == '2025-01-03'
			builderResults[1].sortKey == '2025-01-02'
			builderResults[2].sortKey == '2025-01-01'

		when: 'Using the fluent query builder with filter and projection'
			List<DynamoMap> filteredResults = dynamo
				.query(table, KeyFilter.of('id', 'user1'))
				.filter(match('enabled', true))
				.fields('id', 'sortKey', 'enabled')
				.list()

		then: 'Results should be filtered and projected'
			filteredResults.size() == 2
			filteredResults.every { it.enabled == true }
			filteredResults.every { !it.containsKey('data') } // data field should be excluded

		when: 'Using the fluent scan builder'
			List<TestItem> scanResults = dynamo
				.scan(table, TestItem)
				.filter(match('enabled', true))
				.list()

		then: 'Results should be filtered across all partitions'
			scanResults.size() == 3
			scanResults.every { it.enabled == true }

		when: 'Using paginated query with builder'
			PagedResult<TestItem> pagedResults = dynamo
				.query (
					table,
					KeyFilter.of('id', 'user1'),
					TestItem
				).paged(2)

		then: 'Results should be paginated'
			pagedResults.items.size() == 2
			pagedResults.last != null

		cleanup:
			dynamo.dropTable(table)
	} // }}}

	def "Should handle reserved keywords in field projection"() { // {{{
		given:
			String table = 'test_reserved_keywords'
			String partKey = 'id'
		and:
			dynamo.createTable(table, partKey)
		and:
			// Create a test item with field names that are DynamoDB reserved keywords
			DynamoMap item = new DynamoMap([
				id: 'test1',
				size: '10',     // 'size' is a reserved keyword
				type: 'document',  // 'type' is a reserved keyword
				name: 'test item',  // 'name' is a reserved keyword
				status: 'active'   // 'status' is a reserved keyword
			])
		and:
			KeyFilter key = KeyFilter.of('id', 'test1')
		and:
			dynamo.putItem(table, item)

		when: 'Retrieve item with projection containing reserved keywords'
			DynamoMap projectedItem = dynamo.getItem (
				table,
				key,
				['id', 'size', 'type', 'name'],  // All reserved keywords
				DynamoMap
			)

		then: 'Reserved keyword fields should be returned correctly'
			projectedItem != null
			projectedItem.id == 'test1'
			projectedItem.size == '10'
			projectedItem.type == 'document'
			projectedItem.name == 'test item'
			projectedItem.status == null  // not projected

		when: 'Query with projection containing reserved keywords'
			List<DynamoMap> queryResults = dynamo
				.query(table, key, DynamoMap)
				.fields('id', 'size', 'type')
				.list()

		then: 'Query should succeed with reserved keywords'
			queryResults.size() == 1
			queryResults[0].id == 'test1'
			queryResults[0].size == '10'
			queryResults[0].type == 'document'
			queryResults[0].name == null  // not projected

		when: 'Scan with projection containing reserved keywords'
			List<DynamoMap> scanResults = dynamo
				.scan(table, DynamoMap)
				.fields('id', 'name', 'status')
				.list()

		then: 'Scan should succeed with reserved keywords'
			scanResults.size() == 1
			scanResults[0].id == 'test1'
			scanResults[0].name == 'test item'
			scanResults[0].status == 'active'
			scanResults[0].size == null  // not projected

		cleanup:
			dynamo.dropTable(table)
	} // }}}
}
// vim: fdm=marker
