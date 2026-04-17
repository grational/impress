package it.grational.storage.dynamodb

import it.grational.storage.DbMapper
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import spock.lang.Specification

class Groovy10CompatibilityUSpec extends Specification {

	static class LegacyMapper implements DbMapper<AttributeValue, Object> {
		Map<String, AttributeValue> values = [:]

		@Override
		DbMapper<AttributeValue, Object> with(String k, String s) {
			values[k] = AttributeValue.fromS(s)
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with(String k, Number n) {
			values[k] = AttributeValue.fromN(n.toString())
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with(String k, boolean b) {
			values[k] = AttributeValue.fromBool(b)
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with (
			String k,
			DbMapper<AttributeValue, Object> dm,
			boolean v
		) {
			values[k] = AttributeValue.fromM(dm.storer(v))
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with(String k, String... ls) {
			values[k] = AttributeValue.fromL(ls.collect { AttributeValue.fromS(it) })
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with(String k, Number... ln) {
			values[k] = AttributeValue.fromL (
				ln.collect { AttributeValue.fromN(it.toString()) }
			)
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with (
			String k,
			boolean v,
			Storable<AttributeValue, Object>... ast
		) {
			values[k] = AttributeValue.fromL (
				ast.collect { AttributeValue.fromM(it.impress(new LegacyMapper(), v).storer(v)) }
			)
			this
		}

		@Override
		DbMapper<AttributeValue, Object> with (
			String k,
			boolean v,
			DbMapper<AttributeValue, Object>... adm
		) {
			values[k] = AttributeValue.fromL (
				adm.collect { AttributeValue.fromM(it.storer(v)) }
			)
			this
		}

		@Override
		Map<String, AttributeValue> storer(boolean version) {
			values
		}

		@Override
		Map<String, Object> builder(boolean version) {
			values
		}
	}

	static class LegacyDynable extends Dynable {
		String id
		String name

		LegacyDynable() {}

		LegacyDynable(Map<String, Object> data) {
			super(data)
			id = data.id
			name = data.name
		}

		@Override
		protected DbMapper<AttributeValue, Object> inpress(DynamoMapper mapper) {
			mapper
				.with('id', id, FieldType.PARTITION_KEY)
				.with('name', name)
		}
	}

	static class LegacyStorable implements Storable<AttributeValue, Object> {
		String id
		String name

		@Override
		DbMapper<AttributeValue, Object> impress (
			DbMapper<AttributeValue, Object> mapper,
			boolean versioned
		) {
			mapper
				.with('id', id)
				.with('name', name)
		}
	}

	def "legacy custom DbMapper implementations do not need new 1.1 methods"() {
		given:
			LegacyMapper mapper = new LegacyMapper()
			List<LegacyStorable> items = [
				new LegacyStorable(id: '1', name: 'Ada'),
				new LegacyStorable(id: '2', name: 'Grace')
			]

		when:
			mapper.withItems('items', true, items)

		then:
			mapper.storer().items.l().size() == 2
	}

	def "1.0-style Dynable and DynamoMap Groovy usage still works"() {
		given:
			LegacyDynable domain = new LegacyDynable(id: 'user-1', name: 'Ada')
			DynamoMap dynamic = new DynamoMap(id: 'map-1', name: 'Grace')
				.withPartitionKey('id')

		when:
			DynamoMapper domainMapper = domain.impress(new DynamoMapper()) as DynamoMapper
			DynamoMapper dynamicMapper = dynamic.impress(new DynamoMapper()) as DynamoMapper

		then:
			domainMapper.key().id.s() == 'user-1'
			domainMapper.storer().name.s() == 'Ada'
			dynamicMapper.key().id.s() == 'map-1'
			dynamicMapper.storer().name.s() == 'Grace'
	}
}
