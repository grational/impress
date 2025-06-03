package it.grational.storage.dynamodb

import groovy.transform.ToString
import groovy.transform.CompileStatic
import it.grational.storage.DbMapper
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static it.grational.storage.dynamodb.FieldType.*

@ToString (
	includePackage = false,
	includeFields = true,
	includeNames = true
)
@CompileStatic
class DynamoMap implements Storable<AttributeValue,Object> {

	@Delegate
	Map<String, Object> data
	private String pk
	private String sk

	DynamoMap (
		Map<String, Object> data = [:],
		String pk = null,
		String sk = null
	) {
		this.data = data
		this.pk = pk
		this.sk = sk
	}

	DynamoMap withPartitionKey(String k) {
		this.pk = k
		return this
	}

	DynamoMap withSortKey(String k) {
		this.sk = k
		return this
	}

	DynamoMap withKeys (
		String pk,
		String sk = null
	) {
		this.pk = pk
		this.sk = sk
		return this
	}

	@Override
	DbMapper<AttributeValue,Object> impress (
		DbMapper<AttributeValue,Object> mapper = new DynamoMapper(),
		boolean versioned = false
	) {
		DynamoMapper dm = mapper as DynamoMapper
		data.each { String k, Object v ->
			FieldType ftype = fieldType(k)
			switch (v) {
				case String:
					dm.with(k, v as String, ftype)
					break
				case Number:
					dm.with(k, v as Number, ftype)
					break
				case Boolean:
					dm.with(k, v as Boolean)
					break
				case Map:
					Map<String,Object> mv = v as Map
					dm.with (
						k,
						new DynamoMap(mv).impress (
							new DynamoMapper(),
							versioned
						),
						versioned
					)
					break
				case DbMapper:
					dm.with (
						k,
						(v as DbMapper<AttributeValue,Object>),
						versioned
					)
					break
				case Storable:
					dm.with (
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
						dm.with(k, [] as String[])
						break
					}
					switch (lv[0]) {
						case String:
							dm.with(k, lv as String[])
							break
						case Number:
							dm.with(k, lv as Number[])
							break
						case Map:
							List<DbMapper<AttributeValue,Object>> mappers = lv.collect { Object item ->
								new DynamoMap(item as Map<String,Object>).impress (
									new DynamoMapper(),
									versioned
								)
							}
							dm.with(k, versioned, mappers as DbMapper[])
							break
						case DbMapper:
							dm.with (
								k,
								versioned,
								lv as DbMapper[]
							)
							break
						case Storable:
							dm.with (
								k,
								versioned,
								lv as Storable[]
							)
					}
			}
		}
		return dm
	}

	private FieldType fieldType(String k) {
		return (k == pk)
			? PARTITION_KEY
			: (k == sk)
				? SORT_KEY
				: STANDARD
	}

}
