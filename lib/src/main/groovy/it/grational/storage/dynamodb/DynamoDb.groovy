package it.grational.storage.dynamodb

// imports {{{
import groovy.util.logging.Slf4j
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
// local
import it.grational.storage.Storable
// }}}

@Slf4j
@CompileStatic
class DynamoDb {
	// fields {{{
	private final DynamoDbClient client
	private final static Integer maxTransactions = 25
	// }}}

	DynamoDb(DynamoDbClient client) { // {{{
		this.client = client
	} // }}}

	DynamoDb() { // {{{
		this.client = DynamoDbClient.builder().build()
	} // }}}

	void putItem (
		String table,
		Storable<AttributeValue,Object> storable,
		boolean versioned = true
	) { // {{{
		putItems (
			table,
			[storable],
			versioned
		)
	} // }}}

	void putItems (
		String table,
		List<? extends Storable<AttributeValue,Object>> storables,
		boolean versioned = true
	) { // {{{
		List<DynamoMapper> mappers = storables
		.collect { Storable<AttributeValue,Object> storable ->
			storable.impress (
				new DynamoMapper(),
				versioned
			) as DynamoMapper
		}

		batchPut (
			table,
			mappers
		)
	} // }}}

	private void batchPut (
		String table,
		List<DynamoMapper> mappers
	) { // {{{
		mappers.collate(maxTransactions)
		.each { List<DynamoMapper> batch ->

			List<TransactWriteItem> transactItems = batch
			.collect { DynamoMapper mapper ->

				def putBuilder = Put.builder()
					.tableName(table)

				if ( mapper.hasVersion() ) {
					putBuilder.conditionExpression (
						mapper.versionCondition()
					)
					.expressionAttributeNames (
						mapper.versionNames()
					)
					.expressionAttributeValues (
						mapper.versionValues()
					)
					mapper.incrementVersion()
				}

				TransactWriteItem.builder().put (
					putBuilder.item (
						mapper.storer(false)
					).build()
				).build()
			}

			if ( transactItems.isEmpty() )
				return

			log.debug (
				"Executing transaction with {} items",
				transactItems.size()
			)

			client.transactWriteItems (
				TransactWriteItemsRequest.builder()
					.transactItems(transactItems)
					.build()
			)
		}
	} // }}}


	/**
	 * Updates an item by extracting key attributes from a Storable object
	 *
	 * @param table The table name
	 * @param item The Storable object containing both key and update data (keys will be auto-extracted)
	 * @param versioned Whether to use versioning
	 * @return UpdateItemResponse from DynamoDB
	 */
	UpdateItemResponse updateItem (
		String table,
		Storable<AttributeValue, Object> item,
		boolean versioned = true
	) { // {{{
		DynamoMapper mapper = item.impress (
			new DynamoMapper(),
			versioned
		) as DynamoMapper

		updateItem (
			table,
			mapper
		)
	} // }}}

	UpdateItemResponse updateItem (
		String table,
		DynamoMapper mapper
	) { // {{{
		log.debug (
			String.join(", ",
				"Updating item on table {}",
				"with mapper {}"
			),
			table,
			mapper
		)

		def builder = UpdateItemRequest
			.builder()
			.tableName(table)

		if ( !mapper.hasKey() )
			markKeys(table, mapper)

		builder.key( mapper.key() )

		if ( mapper.hasVersion() ) {
			builder.conditionExpression (
				mapper.versionCondition()
			)
			mapper.incrementVersion()
		}

		UpdateItemRequest request = builder
			.updateExpression (
				mapper.updateExpression()
			)
			.expressionAttributeNames (
				mapper.expressionNames (
					mapper.versionNames()
				)
			)
			.expressionAttributeValues (
				mapper.expressionValues (
					mapper.versionValues()
				)
			)
			.build()

		return client.updateItem(request)
	} // }}}

	UpdateItemResponse removeAttributes (
		String table,
		KeyFilter key,
		String... attributeNames
	) { // {{{
		log.debug (
			String.join(", ",
				"Removing attributes {} from item with key {}",
				"on table {}"
			),
			attributeNames,
			key,
			table
		)

		DynamoMapper mapper = new DynamoMapper()
		mapper.remove(attributeNames)

		def builder = UpdateItemRequest
			.builder()
			.tableName(table)
			.key(key.toMap())

		UpdateItemRequest request = builder
			.updateExpression (
				mapper.updateExpression()
			)
			.expressionAttributeNames (
				mapper.expressionNames()
			)
			.build()

		return client.updateItem(request)
	} // }}}

	<T extends Storable<AttributeValue,Object>> T getItem (
		String table,
		KeyFilter key,
		Class<T> targetClass = DynamoMap.class
	) { // {{{
		getItem(table, key, null, targetClass)
	} // }}}

	<T extends Storable<AttributeValue,Object>> T getItem (
		String table,
		KeyFilter key,
		List<String> fields,
		Class<T> targetClass = DynamoMap.class
	) { // {{{
		log.debug("Getting item with key: {} and projection: {}", key, fields)

		def getItemRequestBuilder = GetItemRequest
			.builder()
			.tableName(table)
			.key(key.toMap())

		if ( fields )
			getItemRequestBuilder.projectionExpression(fields.join(', '))

		def getItemRequest = getItemRequestBuilder.build()
		def response = client.getItem(getItemRequest)

		if (!response.hasItem()) {
			log.debug("No item found for key: {}", key)
			return null
		}

		Map<String, AttributeValue> item = response.item()
		Map<String, Object> builder = new DynamoMapper(item).builder()
		return targetClass.newInstance(builder)
	} // }}}

	/**
	 * Gets an item by extracting key attributes from a sample object
	 *
	 * @param table The table name
	 * @param sampleItem The sample object containing key values (keys will be auto-extracted)
	 * @param targetClass The class to deserialize the result into
	 * @return The item if found, null otherwise
	 */
	<T extends Storable<AttributeValue,Object>> T refreshItem (
		String table,
		Storable<AttributeValue, Object> item,
		List<String> fields = null,
		Class<T> targetClass = DynamoMap.class
	) { // {{{
		Map<String, AttributeValue> key = extractKey (
			table,
			item
		)
		
		log.debug (
			"Getting item with auto-extracted keys: {}",
			key
		)

		def getBuilder = GetItemRequest
			.builder()
			.tableName(table)
			.key(key)

		if ( fields )
			getBuilder.projectionExpression(fields.join(', '))

		def response = client.getItem(getBuilder.build())

		if (!response.hasItem()) {
			log.debug("No item found for auto-extracted keys: {}", key)
			return null
		}

		Map<String, AttributeValue> fresh = response.item()
		Map<String, Object> builder = new DynamoMapper(fresh).builder()
		return targetClass.newInstance(builder)
	} // }}}

	/**
	 * Creates a QueryBuilder for fluent query construction
	 *
	 * @return QueryBuilder for building and executing queries
	 */
	QueryBuilder query (
		String table,
		KeyFilter key
	) {
		return new QueryBuilder (
			this,
			table,
			key
		)
	}

	/**
	 * Creates a ScanBuilder for fluent scan construction
	 *
	 * @return ScanBuilder for building and executing scans
	 */
	ScanBuilder scan(String table) {
		return new ScanBuilder(this, table)
	}

	/**
	 * Helper method to retrieve all results by automatically handling pagination
	 */
	<T extends Storable<AttributeValue,Object>> List<T> queryAll (
		String table,
		String index = null,
		KeyFilter key,
		DynamoFilter filter = null,
		List<String> fields = [],
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		List<T> results = []
		Map<String, AttributeValue> lastEvaluatedKey = [:]
		do {
			PagedResult<T> paged = query (
				table,
				index,
				key,
				filter,
				fields,
				targetClass,
				0,
				lastEvaluatedKey,
				forward
			)
			results.addAll(paged.items)
			lastEvaluatedKey = paged.last
		} while(lastEvaluatedKey)
		
		return results
	} // }}}


	<T extends Storable<AttributeValue,Object>> PagedResult<T> query (
		String table,
		String index,
		KeyFilter key,
		DynamoFilter filter,
		List<String> fields,
		Class<T> targetClass,
		int limit,
		Map<String, AttributeValue> last,
		boolean forward
	) { // {{{
		log.debug (
			String.join(", ",
				"Executing query on table {}",
				"index: {}",
				"key: {}",
				"filter: {}",
				"projection: {}",
				"limit: {}",
				"last: {}",
				"forward: {}"
			),
			table,
			index,
			key,
			filter,
			fields,
			limit,
			last,
			forward
		)

		def queryBuilder = QueryRequest
			.builder()
			.tableName(table)
			.scanIndexForward(forward)

		if ( index )
			queryBuilder.indexName(index)

		queryBuilder
			.keyConditionExpression(key.condition())
			.expressionAttributeNames ( filter
				? ( key.conditionNames() + filter.expressionNames )
				: key.conditionNames()
			)
			.expressionAttributeValues ( filter
				? ( key.conditionValues() + filter.expressionValues )
				: key.conditionValues()
			)

		if ( filter )
			queryBuilder.filterExpression(filter.expression)

		if ( fields )
			queryBuilder.projectionExpression(fields.join(', '))

		if ( limit > 0 )
			queryBuilder.limit(limit)

		if ( last )
			queryBuilder.exclusiveStartKey(last)

		def request = queryBuilder.build()
		log.debug("Executing query request: {}", request)
		
		def response = client.query(request)

		log.debug("Found {} items", response.count())
		
		List<T> items = response.items().collect { item ->
			Map<String,Object> builder = new DynamoMapper(item).builder()
			targetClass.newInstance(builder)
		}

		return new PagedResult<T> (
			items,
			response.lastEvaluatedKey()
		)
	} // }}}

	DeleteItemResponse deleteItem (
		String table,
		KeyFilter key
	) { // {{{
		log.debug("Deleting item with key: {}", key)

		def request = DeleteItemRequest.builder()
			.tableName(table)
			.key(key.toMap())
			.build()

		return client.deleteItem(request)
	} // }}}

	/**
	 * Deletes an item by extracting key attributes from a Storable object
	 *
	 * @param table The table name
	 * @param item The Storable object containing the item data (keys will be auto-extracted)
	 * @return DeleteItemResponse from DynamoDB
	 */
	DeleteItemResponse deleteItem (
		String table,
		Storable<AttributeValue, Object> item
	) { // {{{
		Map<String, AttributeValue> key = extractKey (
			table,
			item
		)
		
		log.debug (
			"Deleting item with auto-extracted keys: {}",
			key
		)

		def request = DeleteItemRequest.builder()
			.tableName(table)
			.key(key)
			.build()

		return client.deleteItem(request)
	} // }}}

	void createTable (
		String table,
		String partition,
		Map<String, String> indexes = [:]
	) { // {{{
		createTable (
			table,
			partition,
			null, // no sort string key
			indexes
		)
	} // }}}

	void createTable (
		String table,
		String partition,
		String sort,
		Map<String, String> indexes = [:]
	) { // {{{
		createTable (
			table,
			partition,
			sort,
			indexes.collect { String idxName, String idxPart ->
				Index.of(Scalar.of(idxPart), null, idxName)
			} as Index[]
		)
	} // }}}

	void createTable (
		String table,
		String partition,
		Index[] indexes
	) { // {{{
		createTable (
			table,
			partition,
			null, // no sort string key
			indexes
		)
	} // }}}

	void createTable (
		String table,
		String partition,
		String sort,
		Index[] indexes
	) { // {{{
		Optional<Scalar> sortKey = sort
			? Optional.of(Scalar.of(sort))
			: Optional.empty()

		createTable (
			table,
			Scalar.of(partition),
			sortKey,
			indexes
		)
	} // }}}

	/**
	 * Creates a DynamoDB table with support for secondary indexes using the Keys and Index classes
	 *
	 * This more advanced version of createTable uses the Scalar, Keys, and Index classes
	 * to provide more control over the table creation process, including key types and index naming.
	 *
	 * Example usage:
	 * <pre>
	 * // Create a table with a string partition key and numeric sort key
	 * dynamoDb.createTable(
	 *   "users",
	 *   Scalar.of("userId", ScalarAttributeType.S),
	 *   Optional.of(Scalar.of("createdAt", ScalarAttributeType.N)),
	 *   [
	 *     Index.of("email"),  // Auto-named as "email-index"
	 *     Index.of("status", "createdAt", "custom-index-name")
	 *   ] as Index[]
	 * )
	 * </pre>
	 *
	 * @param table The name of the table to create
	 * @param partition The Scalar representing the partition key
	 * @param sort Optional Scalar for the sort key
	 * @param indexes Array of Index objects defining secondary indexes
	 * @return void
	 */
	void createTable (
		String table,
		Scalar partition,
		Optional<Scalar> sort = Optional.empty(),
		Index[] indexes
	) { // {{{
		try {
			client.describeTable (
				DescribeTableRequest.builder()
				.tableName(table)
				.build()
			)
			log.warn "Table ${table} already exists"
			return
		} catch (ResourceNotFoundException ignored) {}

		def keySchema = [
			KeySchemaElement.builder()
				.attributeName(partition.name)
				.keyType(KeyType.HASH)
				.build()
		]

		def attributeDefinitions = [
			AttributeDefinition.builder()
				.attributeName(partition.name)
				.attributeType(partition.type)
				.build()
		]

		sort.ifPresent { Scalar ssort ->
			keySchema << KeySchemaElement.builder()
				.attributeName(ssort.name)
				.keyType(KeyType.RANGE)
				.build()

			attributeDefinitions << AttributeDefinition.builder()
				.attributeName(ssort.name)
				.attributeType(ssort.type)
				.build()
		}

		def gsiList = indexes.collect { Index idx ->
			idx.attributes().each { Scalar attribute ->
				if ( attributeDefined(attributeDefinitions, attribute) )
					return

				attributeDefinitions << AttributeDefinition.builder()
					.attributeName(attribute.name)
					.attributeType(attribute.type)
					.build()
			}

			def schema = [
				KeySchemaElement.builder()
					.attributeName(idx.partition.name)
					.keyType(KeyType.HASH)
					.build()
			]

			idx.sort.ifPresent { Scalar ssort ->
				schema << KeySchemaElement.builder()
					.attributeName(ssort.name)
					.keyType(KeyType.RANGE)
					.build()
			}

			GlobalSecondaryIndex.builder()
				.indexName(idx.name)
				.keySchema(schema)
				.projection(
					Projection.builder()
					.projectionType(
						ProjectionType.ALL
					).build()
				).build()
		}

		def builder = CreateTableRequest
			.builder()
			.tableName(table)
			.keySchema(keySchema)
			.attributeDefinitions (
				attributeDefinitions
			)
			.billingMode (
				BillingMode.PAY_PER_REQUEST
			)

		if (gsiList)
			builder.globalSecondaryIndexes(gsiList)

		client.createTable(builder.build())

		client.waiter().waitUntilTableExists (
			DescribeTableRequest
				.builder()
				.tableName(table)
				.build()
		)

		log.info "Table ${table} successfully created"
	} // }}}

	private boolean attributeDefined ( // {{{
		List<AttributeDefinition> definitions,
		Scalar attribute
	) {
		definitions.any { it.attributeName() == attribute.name }
	} // }}}

	void dropTable(String table) { // {{{
		try {
			client.deleteTable (
				DeleteTableRequest.builder()
				.tableName(table)
				.build()
			)
			log.info "Table deleted: ${table}"
		} catch (Exception e) {
			log.error (
				"Error deleting table ${table}: ${e.message}"
			)
		}
	} // }}}

	/**
	 * Deletes multiple items based on query results
	 *
	 * @param table The name of the table to delete items from
	 * @param key The key condition to identify items to delete
	 * @param filter Optional filter to further restrict which items are deleted
	 * @return The number of items deleted
	 */
	int deleteItems (
		String table,
		KeyFilter key,
		DynamoFilter filter = null
	) { // {{{
		deleteItems(table, null, key, filter)
	} // }}}

	/**
	 * Deletes multiple items based on query results using an index
	 *
	 * @param table The name of the table to delete items from
	 * @param index The index name to query on
	 * @param key The key condition to identify items to delete
	 * @param filter Optional filter to further restrict which items are deleted
	 * @return The number of items deleted
	 */
	int deleteItems (
		String table,
		String index,
		KeyFilter key,
		DynamoFilter filter = null
	) { // {{{
		log.debug (
			String.join(", ",
				"Deleting items from table {}",
				"with index: {}",
				"key: {}",
				"and filter: {}"
			),
			table,
			index,
			key,
			filter
		)

		List<DynamoMap> itemsToDelete = queryAll (
			table,
			index,
			key,
			filter,
			null,
			DynamoMap.class,
			true
		)

		if (itemsToDelete.isEmpty()) {
			log.debug("No items found to delete")
			return 0
		}

		List<Map<String, AttributeValue>> itemMaps = itemsToDelete
		.collect { DynamoMap item ->
			item.impress(new DynamoMapper()).storer(false)
		}

		batchDelete(table, itemMaps)

		return itemsToDelete.size()
	} // }}}

	/**
	 * Deletes multiple items based on scan results
	 *
	 * @param table The name of the table to delete items from
	 * @param filter Optional filter to restrict which items are deleted
	 * @return The number of items deleted
	 */
	int deleteItems (
		String table,
		DynamoFilter filter = null
	) { // {{{
		log.debug (
			String.join(", ",
				"Deleting items from table {}",
				"with filter: {}"
			),
			table,
			filter
		)

		List<DynamoMap> itemsToDelete = scanAll (
			table,
			filter,
			DynamoMap.class
		)

		if (itemsToDelete.isEmpty()) {
			log.debug("No items found to delete")
			return 0
		}

		List<Map<String, AttributeValue>> itemMaps = itemsToDelete
		.collect { DynamoMap item ->
			item.impress(new DynamoMapper()).storer(false)
		}

		batchDelete(table, itemMaps)

		return itemsToDelete.size()
	} // }}}

	/**
	 * Batch delete items using transactWriteItems
	 *
	 * @param table The table name
	 * @param items The list of item maps to delete
	 */
	private void batchDelete (
		String table,
		List<Map<String, AttributeValue>> items
	) { // {{{
		log.debug("Batch deleting {} items", items.size())

		items.collate(maxTransactions)
		.each { List<Map<String, AttributeValue>> batch ->
			List<TransactWriteItem> transactItems = batch
			.collect { Map<String, AttributeValue> item ->
				// Extract only the key attributes from the item
				Map<String, AttributeValue> key = extractKey (
					table,
					item
				)

				TransactWriteItem.builder().delete (
					Delete.builder()
						.tableName(table)
						.key(key)
						.build()
				).build()
			}

			if (transactItems.isEmpty()) {
				return
			}

			log.debug(
				"Executing deletion transaction with {} items",
				transactItems.size()
			)

			client.transactWriteItems(
				TransactWriteItemsRequest.builder()
					.transactItems(transactItems)
					.build()
			)
		}
	} // }}}

	/**
	 * Gets the key schema for a table
	 *
	 * @param table The table name
	 * @return The list of key schema elements
	 */
	private List<KeySchemaElement> tableKeySchema(String table) { // {{{
		try {
			DescribeTableResponse tableInfo = client.describeTable (
				DescribeTableRequest.builder()
					.tableName(table)
					.build()
			)
			return tableInfo.table().keySchema()
		} catch (Exception e) {
			log.error("Error getting table key schema for {}: {}", table, e.message)
			throw e
		}
	} // }}}

	/**
	 * Extracts the key attributes from a DynamoDB item
	 *
	 * @param table The table name to identify the key schema
	 * @param item The full item map
	 * @return A map containing only the key attributes
	 */
	Map<String, AttributeValue> extractKey (
		String table,
		Map<String, AttributeValue> item
	) { // {{{
		List<KeySchemaElement> keySchema = tableKeySchema(table)

		// Extract key attributes based on the key schema
		Map<String, AttributeValue> key = [:]

		keySchema.each { KeySchemaElement element ->
			String keyName = element.attributeName()
			if (item.containsKey(keyName)) {
				key[keyName] = item[keyName]
			} else {
				throw new IllegalStateException (
					"Key attribute ${keyName} not found in item"
				)
			}
		}

		return key
	} // }}}

	/**
	 * Extracts key attributes from a Storable object
	 *
	 * @param table The table name to identify the key schema
	 * @param storable The storable object
	 * @return A map containing only the key attributes
	 */
	Map<String, AttributeValue> extractKey (
		String table,
		Storable<AttributeValue, Object> storable,
		boolean versioned = false
	) { // {{{
		return extractKey (
			table,
			storable.impress (
				new DynamoMapper(),
				versioned
			).storer(versioned)
		)
	} // }}}

	/**
	 * Extracts key attributes from a DynamoMapper object
	 *
	 * @param table The table name to identify the key schema
	 * @param mapper The DynamoMapper object containing the full item
	 * @return A map containing only the key attributes
	 */
	void markKeys(String table, DynamoMapper mapper) { // {{{
		tableKeySchema(table)
		.each { KeySchemaElement element ->
			mapper.markAsKey(element)
		}
	} // }}}

	/**
	 * Complete version of scan with all parameters - returns PagedResult for manual pagination control
	 * Note: limit parameter is required to distinguish from List<T> scan methods
	 */
	<T extends Storable<AttributeValue,Object>> PagedResult<T> scan (
		String table,
		DynamoFilter filter,
		List<String> projection,
		Class<T> targetClass,
		int limit,
		Map<String, AttributeValue> last
	) { // {{{
		log.debug (
			String.join(', ',
				'Executing paged scan on table {}',
				'with filter: {}',
				'projection: {}',
				'limit: {}',
				'last: {}'
			),
			table,
			filter,
			projection,
			limit,
			last
		)

		def scanBuilder = ScanRequest.builder()
			.tableName(table)

		if ( filter )
			scanBuilder
			.filterExpression(filter.expression)
			.expressionAttributeNames(filter.expressionNames)
			.expressionAttributeValues(filter.expressionValues)

		if ( projection )
			scanBuilder.projectionExpression(projection.join(', '))

		if ( limit > 0 )
			scanBuilder.limit(limit)

		if ( last )
			scanBuilder.exclusiveStartKey(last)

		ScanRequest request = scanBuilder.build()
		log.debug("Executing scan request: {}", request)
		
		def response = client.scan(request)

		log.debug("Found {} items", response.count())
		
		List<T> items = response.items().collect { item ->
			Map<String,Object> builder = new DynamoMapper(item).builder()
			targetClass.newInstance(builder)
		}

		return new PagedResult<T> (
			items,
			response.lastEvaluatedKey()
		)
	} // }}}

	/**
	 * Helper method to retrieve all results by automatically handling pagination
	 */
	<T extends Storable<AttributeValue,Object>> List<T> scanAll (
		String table,
		DynamoFilter filter = null,
		Class<T> targetClass = DynamoMap.class,
		Integer limit = null,
		Integer segment = null,
		Integer totalSegments = null,
		List<String> projection = null
	) { // {{{
		log.debug (
			String.join(', ',
				'Scanning table {}',
				'with filter: {}',
				'projection: {}',
				'limit: {}',
				'segment: {}/{}'
			),
			table,
			filter,
			projection,
			limit,
			segment,
			totalSegments
		)

		def scanBuilder = ScanRequest.builder()
			.tableName(table)

		if ( filter )
			scanBuilder
			.filterExpression(filter.expression)
			.expressionAttributeNames(filter.expressionNames)
			.expressionAttributeValues(filter.expressionValues)

		if ( projection )
			scanBuilder.projectionExpression(projection.join(', '))

		if ( limit )
			scanBuilder.limit(limit)

		if (segment != null && totalSegments != null)
			scanBuilder
			.segment(segment)
			.totalSegments(totalSegments)

		ScanRequest request = scanBuilder.build()

		def result = []
		ScanIterable scanResponses = client.scanPaginator(request)

		scanResponses.forEach { response ->
			log.debug("Found {} items in scan page", response.count())
			response.items().each { item ->
				Map<String, Object> builder = new DynamoMapper(item).builder()
				result << targetClass.newInstance(builder)
			}
		}

		log.debug("Total items found in scan: {}", result.size())
		return result
	} // }}}

}
// vim: fdm=marker
