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

	DynamoMap(Map<String, Object> data = [:]) {
		this.data = data
	}

	@Override
	DbMapper<AttributeValue,Object> impress (
		DbMapper<AttributeValue,Object> mapper = new DynamoMapper(),
		boolean versioned = false
	) {
		data.each { String k, Object v ->
			switch (v) {
				case String:
					mapper.with(k, v as String)
					break
				case Number:
					mapper.with(k, v as Number)
					break
				case Boolean:
					mapper.with(k, v as Boolean)
					break
				case Map:
					Map<String,Object> mv = v as Map
					mapper.with (
						k,
						new DynamoMap(mv).impress (
							new DynamoMapper(),
							versioned
						),
						versioned
					)
					break
				case DbMapper:
					mapper.with (
						k,
						(v as DbMapper<AttributeValue,Object>),
						versioned
					)
					break
				case Storable:
					mapper.with (
						k,
						(v as Storable<AttributeValue,Object>).impress (
							new DynamoMapper(),
							versioned
						),
						versioned
					)
					break
				case List:
					List lv = v as List<Object>
					if (lv.isEmpty()) {
						mapper.with(k, [] as String[])
						break
					}
					switch (lv[0]) {
						case String:
							mapper.with(k, lv as String[])
							break
						case Number:
							mapper.with(k, lv as Number[])
							break
						case Map:
							List<DbMapper<AttributeValue,Object>> mappers = lv.collect { Object item ->
								new DynamoMap(item as Map<String,Object>).impress (
									new DynamoMapper(),
									versioned
								)
							}
							mapper.with(k, versioned, mappers as DbMapper[])
							break
						case DbMapper:
							mapper.with (
								k,
								versioned,
								lv as DbMapper[]
							)
							break
						case Storable:
							mapper.with (
								k,
								versioned,
								lv as Storable[]
							)
					}
			}
		}
		return mapper
	}

}
