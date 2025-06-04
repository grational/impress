package it.grational.storage.dynamodb

// imports {{{
import it.grational.storage.Storable
import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

@CompileStatic
class ScanBuilder<T extends Storable<AttributeValue, Object>> {
	// fields {{{
	private final DynamoDb dynamoDb
	private String table
	private DynamoFilter filter
	private List<String> fields
	private Class<T> type = DynamoMap.class as Class<T>
	private Integer limit
	private Integer segment
	private Integer totalSegments
	// }}}
	
	// constructor {{{
	ScanBuilder (
		DynamoDb dynamoDb,
		String table
	) {
		this.dynamoDb = dynamoDb
		this.table = table
	}
	// }}}
	
	// helpers {{{
	ScanBuilder<T> filter(DynamoFilter filter) {
		this.filter = filter
		return this
	}
	
	ScanBuilder<T> fields(List<String> fields) {
		this.fields = fields
		return this
	}
	
	ScanBuilder<T> fields(String... fields) {
		this.fields = fields.toList()
		return this
	}
	
	ScanBuilder<T> type(Class<? extends T> type) {
		this.type = (Class<T>) type
		return this
	}
	
	ScanBuilder<T> limit(Integer limit) {
		this.limit = limit
		return this
	}
	
	ScanBuilder<T> segment (
		Integer segment,
		Integer totalSegments
	) {
		this.segment = segment
		this.totalSegments = totalSegments
		return this
	}
	// }}}
	
	List<T> list() { // {{{
		return dynamoDb.scanAll (
			table,
			filter,
			type,
			limit,
			segment,
			totalSegments,
			fields
		)
	} // }}}
	
	PagedResult<T> paged ( // {{{
		int pageLimit,
		Map<String, AttributeValue> lastKey = null
	) {
		return dynamoDb.scan (
			table,
			filter,
			fields,
			type,
			pageLimit,
			lastKey
		)
	} // }}}

}
// vim: fdm=marker
