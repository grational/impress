package it.grational.storage.dynamodb

// imports {{{
import it.grational.storage.Storable
import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

@CompileStatic
class GetItemBuilder<T extends Storable<AttributeValue, Object>> {
	// fields {{{
	private final DynamoDb dynamo
	private String table
	private KeyFilter key
	private List<String> fields
	private Class<T> type
	// }}}
	
	// constructor {{{
	GetItemBuilder (
		DynamoDb dynamo,
		String table,
		KeyFilter key,
		Class<T> type = DynamoMap.class
	) {
		this.dynamo = dynamo
		this.table = table
		this.key = key
		this.type = type
	}
	// }}}
	
	// helpers {{{
	GetItemBuilder<T> as(Class<T> type) {
		this.type = type
		return this
	}

	GetItemBuilder<T> fields(List<String> fields) {
		this.fields = fields
		return this
	}
	
	GetItemBuilder<T> fields(String... fields) {
		this.fields = fields.toList()
		return this
	}
	// }}}
	
	T get() { // {{{
		return dynamo.getItem (
			table,
			key,
			fields,
			type
		)
	} // }}}

}
// vim: fdm=marker
