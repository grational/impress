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
			.key(mapper.key())

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
	 * Query a table using just a partition key - returns all results with automatic pagination
	 *
	 * Just a convenience method using the partition key 
	 * of the provided composite key
	 *
	 * @param table The table name to query
	 * @param key The KeyFilter representing the composite key condition
	 * @param targetClass The class to deserialize results into
	 * @param forward Whether to sort in forward (true) or reverse (false) order
	 * @return A list of objects of the specified target class
	 */
	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		KeyFilter key,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			null, // no index is needed
			key,
			null,
			null, // no projection
			targetClass,
			forward
		)
	} // }}}

	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		KeyFilter key,
		List<String> fields,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			null, // no index is needed
			key,
			null, // no filter
			fields,
			targetClass,
			forward
		)
	} // }}}

	/**
	 * Query a table using just a partition key - returns all results with automatic pagination
	 *
	 * This is a convenience method that uses the partition component of the provided key
	 *
	 * @param table The table name to query
	 * @param key The KeyFilter representing the key condition
	 * @param filter Optional filter expression to apply to results
	 * @param targetClass The class to deserialize results into
	 * @param forward Whether to sort in forward (true) or reverse (false) order
	 * @return A list of objects of the specified target class
	 */
	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		KeyFilter key,
		DynamoFilter filter,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			null, // no index is needed
			key,
			filter,
			null, // no projection
			targetClass,
			forward
		)
	} // }}}

	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		KeyFilter key,
		List<String> fields,
		DynamoFilter filter,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			null, // no index is needed
			key,
			filter,
			fields,
			targetClass,
			forward
		)
	} // }}}

	/**
	 * Query objects using an index (no filters version) - returns all results with automatic pagination
	 */
	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		String index,
		KeyFilter key,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			index,
			key,
			null,
			null, // no projection
			targetClass,
			forward
		)
	} // }}}

	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		String index,
		KeyFilter key,
		List<String> fields,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			index,
			key,
			null, // no filter
			fields,
			targetClass,
			forward
		)
	} // }}}

	/**
	 * Query objects using an index (with filters version) - returns all results with automatic pagination
	 */
	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		String index,
		KeyFilter key,
		DynamoFilter filter,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			index,
			key,
			filter,
			null, // no projection
			targetClass,
			forward
		)
	} // }}}

	<T extends Storable<AttributeValue,Object>> List<T> query (
		String table,
		String index,
		KeyFilter key,
		List<String> fields,
		DynamoFilter filter,
		Class<T> targetClass = DynamoMap.class,
		boolean forward = true
	) { // {{{
		return queryAll (
			table,
			index,
			key,
			filter,
			fields,
			targetClass,
			forward
		)
	} // }}}


	/**
	 * Convenience methods for query with PagedResult return type
	 */
	<T extends Storable<AttributeValue,Object>> PagedResult<T> query (
		String table,
		KeyFilter key,
		Class<T> targetClass,
		int limit
	) { // {{{
		return query (
			table,
			null, // no index
			key,
			null, // no filter
			null, // no projection
			targetClass,
			limit,
			null, // no last key
			true  // forward
		)
	} // }}}

	<T extends Storable<AttributeValue,Object>> PagedResult<T> query (
		String table,
		String index,
		KeyFilter key,
		Class<T> targetClass,
		int limit
	) { // {{{
		return query (
			table,
			index,
			key,
			null, // no filter
			null, // no projection
			targetClass,
			limit,
			null, // no last key
			true  // forward
		)
	} // }}}

	<T extends Storable<AttributeValue,Object>> PagedResult<T> query (
		String table,
		KeyFilter key,
		DynamoFilter filter,
		Class<T> targetClass,
		int limit
	) { // {{{
		return query (
			table,
			null, // no index
			key,
			filter,
			null, // no projection
			targetClass,
			limit,
			null, // no last key
			true  // forward
		)
	} // }}}

	/**
	 * Helper method to retrieve all results by automatically handling pagination
	 */
	private <T extends Storable<AttributeValue,Object>> List<T> queryAll (
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

	/**
	 * Complete version of query with all parameters - returns PagedResult for manual pagination control
	 * Note: limit parameter is required to distinguish from List<T> query methods
	 */
	<T extends Storable<AttributeValue,Object>> PagedResult<T> query (
		String table,
		String index,
		KeyFilter key,
		DynamoFilter filter,
		Class<T> targetClass,
		int limit,
		Map<String, AttributeValue> last,
		boolean forward
	) { // {{{
		return query(table, index, key, filter, null, targetClass, limit, last, forward)
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

		List<DynamoMap> itemsToDelete = query (
			table,
			index,
			key,
			filter
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

		List<DynamoMap> itemsToDelete = scan (
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
				Map<String, AttributeValue> keyAttributes = extractKeyAttributes(table, item)

				TransactWriteItem.builder().delete (
					Delete.builder()
						.tableName(table)
						.key(keyAttributes)
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
	 * Extracts the key attributes from a DynamoDB item
	 *
	 * @param table The table name to identify the key schema
	 * @param item The full item map
	 * @return A map containing only the key attributes
	 */
	private Map<String, AttributeValue> extractKeyAttributes (
		String table,
		Map<String, AttributeValue> item
	) { // {{{
		try {
			// Describe the table to get the key schema
			DescribeTableResponse tableInfo = client.describeTable (
				DescribeTableRequest.builder()
					.tableName(table)
					.build()
			)

			List<KeySchemaElement> keySchema = tableInfo.table().keySchema()

			// Extract key attributes based on the key schema
			Map<String, AttributeValue> keyAttributes = [:]

			keySchema.each { KeySchemaElement element ->
				String keyName = element.attributeName()
				if (item.containsKey(keyName)) {
					keyAttributes[keyName] = item[keyName]
				} else {
					throw new IllegalStateException (
						"Key attribute ${keyName} not found in item"
					)
				}
			}

			return keyAttributes
		} catch (Exception e) {
			log.error("Error extracting key attributes: {}", e.message)
			// Fallback: return the entire item, though this may fail
			return item
		}
	} // }}}

	/**
	 * Scans the entire table and returns items that match the optional filter expression
	 *
	 * @param table The name of the table to scan
	 * @param targetClass The class of objects to create from the scan results
	 * @param filter Optional DynamoFilter to filter the scan results
	 * @param limit Optional maximum number of items to evaluate
	 * @param segment Optional segment number (for parallel scans)
	 * @param totalSegments Optional total number of segments (for parallel scans)
	 * @return A list of objects of type T created from the scan results
	 */
	<T extends Storable<AttributeValue,Object>> List<T> scan (
		String table,
		DynamoFilter filter = null,
		Class<T> targetClass = DynamoMap.class,
		Integer limit = null,
		Integer segment = null,
		Integer totalSegments = null
	) { // {{{
		log.debug (
			String.join(', ',
				'Scanning table {}',
				'with filter: {}',
				'limit: {}',
				'segment: {}/{}'
			),
			table,
			filter,
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
