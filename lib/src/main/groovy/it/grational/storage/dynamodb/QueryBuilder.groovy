package it.grational.storage.dynamodb

// imports {{{
import it.grational.storage.Storable
import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

@CompileStatic
class QueryBuilder<T extends Storable<AttributeValue, Object>> {
	// fields {{{
	private final DynamoDb dynamoDb
	private String table
	private String index
	private KeyFilter key
	private DynamoFilter filter
	private List<String> fields
	private Class<T> type
	private boolean forward = true
	// }}}

	// constructor {{{
	QueryBuilder (
		DynamoDb dynamoDb,
		String table,
		KeyFilter key,
		Class<T> type
	) {
		this.dynamoDb = dynamoDb
		this.table = table
		this.key = key
		this.type = type
	}
	// }}}

	// constructor {{{
	QueryBuilder (
		DynamoDb dynamoDb,
		String table,
		String index,
		KeyFilter key,
		Class<T> type = DynamoMap.class
	) {
		this.dynamoDb = dynamoDb
		this.table = table
		this.index = index
		this.key = key
		this.type = type
	}
	// }}}

	// helpers {{{
	QueryBuilder<T> filter(DynamoFilter filter) {
		this.filter = filter
		return this
	}

	QueryBuilder<T> fields(List<String> fields) {
		this.fields = fields
		return this
	}

	QueryBuilder<T> fields(String... fields) {
		this.fields = fields.toList()
		return this
	}

	QueryBuilder<T> forward(boolean forward = true) {
		this.forward = forward
		return this
	}

	QueryBuilder<T> backward() {
		this.forward = false
		return this
	}
	// }}}

	List<T> list() { // {{{
		return dynamoDb.queryAll (
			table,
			index,
			key,
			filter,
			fields,
			type,
			forward
		)
	} // }}}

	PagedResult<T> paged ( // {{{
		int limit,
		Map<String, AttributeValue> lastKey = null
	) {
		return dynamoDb.query (
			table,
			index,
			key,
			filter,
			fields,
			type,
			limit,
			lastKey,
			forward
		)
	} // }}}

	PagedResult<T> paged ( // {{{
		int limit,
		Map<String, AttributeValue> lastKey,
		boolean forward
	) {
		return dynamoDb.query (
			table,
			index,
			key,
			filter,
			fields,
			type,
			limit,
			lastKey,
			forward
		)
	} // }}}

}
// vim: fdm=marker
