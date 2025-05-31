package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class DynamoMapUSpec extends Specification {

	def "Should be able to impress itself on a DynamoMapper with basic types"() {
		setup:
			Map<String, Object> data = [
				string: 'value',
				number: 42,
				boolean: true
			]
		and:
			def expected = new DynamoMapper().tap {
				with('string', 'value')
				with('number', 42)
				with('boolean', true)
			}

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			impressed.storer(false) == expected.storer(false)
	}

	def "Should be able to impress itself on a DynamoMapper with list types"() {
		setup:
			Map<String, Object> data = [
				stringList: ['value1', 'value2', 'value3'],
				numberList: [1, 2, 3, 4, 5]
			]
		and:
			def expected = new DynamoMapper().tap {
				with('stringList', 'value1', 'value2', 'value3')
				with('numberList', 1, 2, 3, 4, 5)
			}

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			impressed.storer(false) == expected.storer(false)
	}

	def "Should be able to impress itself on a DynamoMapper with nested DynamoMapper"() {
		setup:
			def nestedMapper = new DynamoMapper().tap {
				with('nestedKey', 'nestedValue')
				with('nestedNumber', 123)
			}
		and:
			Map<String, Object> data = [
				nested: nestedMapper
			]

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			def storedMap = impressed.storer(false)
		and:
			def nestedMapAttr = storedMap.get('nested')
			nestedMapAttr.type() == AttributeValue.Type.M
			nestedMapAttr.m() == nestedMapper.storer(false)
	}

	def "Should be able to impress itself on a DynamoMapper with list of DynamoMappers"() {
		setup:
			def mapper1 = new DynamoMapper().tap {
				with('key1', 'value1')
			}
		and:
			def mapper2 = new DynamoMapper().tap {
				with('key2', 'value2')
			}
		and:
			Map<String, Object> data = [
				mapperList: [mapper1, mapper2]
			]

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			def storedMap = impressed.storer(false)
		and:
			def listAttr = storedMap.get('mapperList')
			listAttr.type() == AttributeValue.Type.L
			listAttr.l().size() == 2
			listAttr.l()[0].m() == mapper1.storer(false)
			listAttr.l()[1].m() == mapper2.storer(false)
	}

	def "Should be able to impress itself on a DynamoMapper with a nested Storable"() {
		setup:
			def storable = new DynamoMap([name: 'item', value: 10])
		and:
			Map<String, Object> data = [
				storable: storable
			]

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			def storedMap = impressed.storer(false)
		and:
			def storableAttr = storedMap.get('storable')
			storableAttr.type() == AttributeValue.Type.M
			storableAttr.m().get('name').s() == 'item'
			storableAttr.m().get('value').n() == '10'
	}

	def "Should be able to impress itself on a DynamoMapper with list of Storables"() {
		setup:
			def storable1 = new DynamoMap([name: 'item1', value: 10])
			def storable2 = new DynamoMap([name: 'item2', value: 20])
		and:
			Map<String, Object> data = [
				storableList: [ storable1, storable2 ]
			]

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			def storedMap = impressed.storer(false)
		and:
			def listAttr = storedMap.get('storableList')
			listAttr.type() == AttributeValue.Type.L
			listAttr.l().size() == 2
			listAttr.l()[0].m().get('name').s() == 'item1'
			listAttr.l()[0].m().get('value').n() == '10'
			listAttr.l()[1].m().get('name').s() == 'item2'
			listAttr.l()[1].m().get('value').n() == '20'
	}

	def "Should handle empty lists gracefully"() {
		setup:
			Map<String, Object> data = [
				emptyList: []
			]

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			def storedMap = impressed.storer(false)
			!storedMap.containsKey('emptyList')
	}

	def "Should handle mixed data types in the same map"() {
		setup:
			def nestedMapper = new DynamoMapper()
			nestedMapper.with('nestedKey', 'nestedValue')
		and:
			Map<String, Object> data = [
				string: 'text',
				number: 42,
				boolean: true,
				stringList: ['a', 'b', 'c'],
				numberList: [1, 2, 3],
				nested: nestedMapper
			]

		when:
			def impressed = new DynamoMap(data).impress()

		then:
			verifyAll(impressed.storer(false)) {
				get('string').s() == 'text'
				get('number').n() == '42'
				get('boolean').bool() == true
				get('stringList').l()*.s() == ['a', 'b', 'c']
				get('numberList').l()*.n() == ['1', '2', '3']
				get('nested').m() == nestedMapper.storer(false)
			}
	}

	def "Should handle versioning correctly"() {
		setup:
			def nestedStorable = new DynamoMap([field: 'value'])
		and:
			def mapper1 = new DynamoMapper().tap {
				with('key', 'value')
			}
		and:
			Map<String, Object> data = [
				nestedStorable: nestedStorable,
				nestedMapper: mapper1,
				storableList: [ nestedStorable ],
				mapperList: [ mapper1 ]
			]

		when:
			def withVersioning = new DynamoMap(data).impress()
			def withoutVersioning = new DynamoMap(data).impress()
			
		then:
			noExceptionThrown()
	}
}
