package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.*

/** Conversion only: no network calls and no mutation of domain objects. */
@CompileStatic
class TransactionItems {
	static TransactWriteItem put(String table, Storable<AttributeValue, Object> item, boolean versioned) {
		DynamoMapper mapper = item.impress(new DynamoMapper(), versioned) as DynamoMapper
		def put = Put.builder().tableName(table)
		if (mapper.hasVersion()) {
			String name = mapper.versionNames().keySet().first()
			String value = mapper.versionValues().keySet().first()
			boolean initial = new BigDecimal(mapper.versionValues()[value].n()) == 0
			put.conditionExpression(initial ? mapper.versionCondition() : ("$name = $value" as String))
				.expressionAttributeNames(mapper.versionNames()).expressionAttributeValues(mapper.versionValues())
			mapper.incrementVersion()
		}
		return TransactWriteItem.builder().put(put.item(mapper.storer(false)).build()).build()
	}

	static TransactWriteItem put(String table, Storable<AttributeValue, Object> item, DynamoFilter condition) {
		def put = Put.builder().tableName(table).item(item.impress(new DynamoMapper(), false).storer())
			.conditionExpression(condition.expression).expressionAttributeNames(condition.expressionNames)
		if (condition.expressionValues)
			put.expressionAttributeValues(condition.expressionValues)
		return TransactWriteItem.builder().put(put.build()).build()
	}

	static TransactWriteItem delete(String table, KeyFilter key, DynamoFilter condition) {
		def deletion = Delete.builder().tableName(table).key(key.toMap())
			.conditionExpression(condition.expression).expressionAttributeNames(condition.expressionNames)
		if (condition.expressionValues)
			deletion.expressionAttributeValues(condition.expressionValues)
		return TransactWriteItem.builder().delete(deletion.build()).build()
	}

	static TransactWriteItem update(String table, KeyFilter key, DynamoMapper changes, DynamoFilter condition) {
		def update = Update.builder().tableName(table).key(key.toMap())
			.updateExpression(changes.updateExpression()).conditionExpression(condition.expression)
			.expressionAttributeNames(changes.expressionNames(condition.expressionNames))
		Map<String, AttributeValue> values = changes.expressionValues(condition.expressionValues)
		if (values)
			update.expressionAttributeValues(values)
		return TransactWriteItem.builder().update(update.build()).build()
	}
}
