package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import it.grational.storage.DbMapper
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

@CompileStatic
interface DynamoDbMapper extends DbMapper<AttributeValue, Object> {
	@Override
	DynamoDbMapper with(String k, String s)
	@Override
	DynamoDbMapper with(String k, Number n)
	@Override
	DynamoDbMapper with(String k, boolean b)
	@Override
	DynamoDbMapper with(String k, DbMapper<AttributeValue, Object> dm, boolean version)
	@Override
	DynamoDbMapper with(String k, String... ls)
	@Override
	DynamoDbMapper with(String k, Number... ln)
	@Override
	DynamoDbMapper with(String k, boolean v, Storable<AttributeValue, Object>... ast)
	@Override
	DynamoDbMapper with(String k, boolean v, DbMapper<AttributeValue, Object>... adm)
	default DynamoDbMapper with (
		String k,
		DbMapper<AttributeValue, Object> mapper
	) {
		return with(k, mapper, true)
	}
	DynamoDbMapper withItem(String k, Storable<AttributeValue, Object> item)
	DynamoDbMapper withItem(String k, Storable<AttributeValue, Object> item, boolean versioned)
	DynamoDbMapper with(String k, String s, FieldType t)
	DynamoDbMapper with(String k, Number n, FieldType t)
	@Override
	DynamoDbMapper withItems(String k, boolean v, Iterable<? extends Storable<AttributeValue, Object>> ast)
	default DynamoDbMapper withItems (
		String k,
		Iterable<? extends Storable<AttributeValue, Object>> items
	) {
		return withItems(k, true, items)
	}
	@Override
	DynamoDbMapper withMappers(String k, boolean v, Collection<? extends DbMapper<AttributeValue, Object>> adm)
	default DynamoDbMapper withMappers (
		String k,
		Collection<? extends DbMapper<AttributeValue, Object>> mappers
	) {
		return withMappers(k, true, mappers)
	}
	DynamoDbMapper withNull(String k)
	DynamoDbMapper remove(String... attributeNames)
}
