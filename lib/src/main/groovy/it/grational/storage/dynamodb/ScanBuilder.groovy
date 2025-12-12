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
	private Class<T> type
	private Integer limit
	private Integer takeResults
	private Integer segment
	private Integer totalSegments
	// }}}
	
	// constructor {{{
	ScanBuilder (
		DynamoDb dynamoDb,
		String table,
		Class<T> type = DynamoMap.class
	) {
		this.dynamoDb = dynamoDb
		this.table = table
		this.type = type
	}
	// }}}
	
	// helpers {{{
	ScanBuilder<T> as(Class<T> type) {
		this.type = type
		return this
	}

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
	
	ScanBuilder<T> limit(Integer limit) {
		this.limit = limit
		return this
	}
	
	ScanBuilder<T> take(Integer takeResults) {
		this.takeResults = takeResults
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
		if (takeResults != null) {
			return dynamoDb.scanAllWithTake (
				table,
				filter,
				type,
				limit,
				segment,
				totalSegments,
				fields,
				takeResults
			)
		} else {
			return dynamoDb.scanAll (
				table,
				filter,
				type,
				limit,
				segment,
				totalSegments,
				fields
			)
		}
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
