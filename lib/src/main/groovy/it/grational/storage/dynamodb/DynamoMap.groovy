package it.grational.storage.dynamodb

import groovy.transform.ToString
import groovy.transform.CompileStatic
import it.grational.storage.DbMapper
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

@ToString (
	includePackage = false,
	includeFields = true,
	includeNames = true
)
@CompileStatic
class DynamoMap implements Storable<AttributeValue,Object> {

	Map<String, Object> data

	DynamoMap() {}

	DynamoMap(Map<String, Object> data) {
		this.data = data
	}

	@Override
	DbMapper<AttributeValue,Object> impress (
		DbMapper<AttributeValue,Object> mapper,
		boolean versioned
	) {
		return mapper
	}
}
