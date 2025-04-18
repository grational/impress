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

	<T extends Storable<AttributeValue,Object>> T objectByKey (
		String table,
		DynamoKey key,
		Class<T> targetClass
	) { // {{{
		log.debug("Getting item with key: {}", key)

		def getItemRequest = GetItemRequest
			.builder()
			.tableName(table)
			.key(key.toMap())
			.build()

		def response = client.getItem(getItemRequest)

		if (!response.hasItem()) {
			log.debug("No item found for key: {}", key)
			return null
		}

		Map<String, AttributeValue> item = response.item()
		Map<String, Object> builder = new DynamoMapper(item).builder()
		return targetClass.newInstance(builder)
	} // }}}

	<T extends Storable<AttributeValue,Object>> List<T> objectsByIndex (
		String table,
		String index,
		DynamoKey key,
		Class<T> targetClass,
		DynamoFilter filter = null
	) { // {{{
		def queryBuilder = QueryRequest.builder()
			.tableName(table)
			.indexName(index)
			.keyConditionExpression(key.condition())
			.expressionAttributeNames ( filter
				? ( key.conditionNames() + filter.expressionNames )
				: key.conditionNames()
			)
			.expressionAttributeValues ( filter
				? ( key.conditionValues() + filter.expressionValues )
				: key.conditionValues()
			)
			
		// Applica il filtro se presente
		if ( filter )
			queryBuilder.filterExpression(filter.expression)
		
		def request = queryBuilder.build()
		log.debug("Executing query request: {}", request)
		def response = client.query(request)

		log.debug("Found {} items", response.count())
		return response.items().collect { item ->
			Map<String,Object> builder = new DynamoMapper(item).builder()
			targetClass.newInstance(builder)
		}
	} // }}}

	DeleteItemResponse deleteItem (
		String table,
		DynamoKey key
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
		String partitionKey,
		String sortKey = null,
		Map<String, String> indexes = [:]
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
				.attributeName(partitionKey)
				.keyType(KeyType.HASH)
				.build()
		]

		if (sortKey) {
			keySchema << KeySchemaElement.builder()
				.attributeName(sortKey)
				.keyType(KeyType.RANGE)
				.build()
		}

		def attributeDefinitions = [
			AttributeDefinition.builder()
				.attributeName(partitionKey)
				.attributeType(ScalarAttributeType.S)
				.build()
		]

		if (sortKey) {
			attributeDefinitions << AttributeDefinition.builder()
				.attributeName(sortKey)
				.attributeType(ScalarAttributeType.S)
				.build()
		}

		indexes.each { indexName, indexKey ->
			if (!attributeDefinitions.any {
				it.attributeName() == indexKey
			}) {
				attributeDefinitions << AttributeDefinition.builder()
					.attributeName(indexKey)
					.attributeType(ScalarAttributeType.S)
					.build()
			}
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

		if (indexes) {
			def gsiList = indexes.collect { indexName, indexKey ->
				GlobalSecondaryIndex.builder()
				.indexName(indexName)
				.keySchema([
					KeySchemaElement.builder()
					.attributeName(indexKey)
					.keyType(KeyType.HASH)
					.build()
				])
				.projection (
					Projection.builder()
					.projectionType (
						ProjectionType.ALL
					).build()
				).build()
			}
			builder.globalSecondaryIndexes (
				gsiList
			)
		}

		client.createTable(builder.build())

		client.waiter().waitUntilTableExists (
			DescribeTableRequest
				.builder()
				.tableName(table)
				.build()
		)

		log.info "Table ${table} successfully created"
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
		Class<T> targetClass,
		DynamoFilter filter = null,
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
