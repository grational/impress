package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import it.grational.storage.DbMapper
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

@CompileStatic
interface DynamoStorable extends Storable<AttributeValue, Object> {
	@Override
	DynamoDbMapper impress (
		DbMapper<AttributeValue, Object> mapper,
		boolean versioned
	)
}
