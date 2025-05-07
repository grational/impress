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

	@Delegate
	Map<String, Object> data

	DynamoMap() {}

	DynamoMap(Map<String, Object> data) {
		this.data = data
	}

	@Override
	DbMapper<AttributeValue,Object> impress (
		DbMapper<AttributeValue,Object> mapper = new DynamoMapper(),
		boolean versioned = false
	) {
		data.each { String key, value ->
			switch (value) {
				case String:
					mapper.with(key, value as String)
					break
				case Number:
					mapper.with(key, value as Number)
					break
				case Boolean:
					mapper.with(key, value as Boolean)
					break
				case List:
					List lv = value as List
					if (lv.isEmpty())
						break
					switch (lv[0]) {
						case String:
							mapper.with (
								key,
								value as String[]
							)
							break
						case Number:
							mapper.with (
								key,
								value as Number[]
							)
							break
						case Storable:
							mapper.with (
								key,
								versioned,
								value as Storable[]
							)
							break
						case DbMapper:
							mapper.with (
								key,
								versioned,
								value as DbMapper[]
							)
							break
					}
					break
				case DbMapper:
					mapper.with (
						key,
						value as DbMapper,
						versioned
					)
					break
				case Storable:
					def dynamoMapper = new DynamoMapper()
					(value as Storable).impress(dynamoMapper, versioned)
					mapper.with(key, dynamoMapper, versioned)
			}
		}
		return mapper
	}
}
