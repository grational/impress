package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*

class DynamoMapperUSpec extends Specification {

	def "Should support all the types of attributes"() { // {{{
		when:
			def mapper = new DynamoMapper().tap {
				with('string', 'value')
				with('number', 1)
				with('boolean', true)
				with('stringSet', ['value1', 'value2'])
				withNumbers('numberSet', [1, 2])
			}
		then:
			mapper.storer() == [
				string:    fromS('value'),
				number:    fromN('1'),
				boolean:   fromBool(true),
				stringSet: fromSs(['value1', 'value2']),
				numberSet: fromNs(['1', '2'])
			]
	} // }}}

	def "Should support nested mappers"() { // {{{
		given:
			def nested = new DynamoMapper().tap {
				with('string', 'value')
				with('number', 1)
				with('boolean', true)
				with('stringSet', ['value1', 'value2'])
				withNumbers('numberSet', [1, 2])
			}

		when:
			def mapper = new DynamoMapper().tap {
				with('nested', nested)
			}

		then:
			mapper.storer() == [
				nested: fromM([
					string:    fromS('value'),
					number:    fromN('1'),
					boolean:   fromBool(true),
					stringSet: fromSs(['value1', 'value2']),
					numberSet: fromNs(['1', '2'])
				])
			]
	} // }}}

	def "Should be able to return a builder map from a storer"() { // {{{
		given:
			Map<String, AttributeValue> storer = [
				string:    fromS('value'),
				number:    fromN('1'),
				boolean:   fromBool(true),
				stringSet: fromSs(['value1', 'value2']),
				numberSet: fromNs(['1', '2'])
			]

		when:
			def builder = new DynamoMapper(storer).builder()

		then:
			builder['string'] == 'value'
			builder['number'] == 1
			builder['boolean'] == true
			builder['stringSet'].first() == 'value1'
			builder['stringSet'].last()  == 'value2'
			builder['numberSet'].first() == 1
			builder['numberSet'].last()  == 2
	} // }}}

	def "Should be able to return a nested builder map from a storer"() { // {{{
		given:
			Map<String, AttributeValue> storer = [
				nested: fromM([
					string:    fromS('value'),
					number:    fromN('1'),
					boolean:   fromBool(true),
					stringSet: fromSs(['value1', 'value2']),
					numberSet: fromNs(['1', '2'])
				])
			]

		when:
			def builder = new DynamoMapper(storer).builder()
			def nested = builder.nested

		then:
			nested['string'] == 'value'
			nested['number'] == 1
			nested['boolean'] == true
			nested['stringSet'].first() == 'value1'
			nested['stringSet'].last()  == 'value2'
			nested['numberSet'].first() == 1
			nested['numberSet'].last()  == 2
	} // }}}

	def "Should correctly tell if it has a key"() { // {{{
		when:
			def mapper = new DynamoMapper().tap {
				with('key', 'value', FieldType.PARTITION_KEY)
			}
		then:
			mapper.hasKey() == true

		when:
			mapper = new DynamoMapper().tap {
				with('key', 'value')
			}
		then:
			mapper.hasKey() == false
	} // }}}

	def "Should be capable of retain partition and sort keys"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('partitionKey', 'one', FieldType.PARTITION_KEY )
				with('sortKey', 2, FieldType.SORT_KEY )
				with('dataField', true)
			}

		expect:
			mapper.key() == expected.findAll { it.key in ['partitionKey', 'sortKey'] }
		and:
			mapper.storer() == expected

		where:
			expected = [
				partitionKey: fromS('one'),
				sortKey:      fromN('2'),
				dataField:    fromBool(true)
			]
	} // }}}

	def "Should correctly tell if it has a version"() { // {{{
		when:
			def mapper = new DynamoMapper().tap {
				with('v', '1')
			}
		then:
			mapper.hasVersion() == false

		when:
			mapper = new DynamoMapper().tap {
				with('v', '1', FieldType.VERSION)
			}
		then:
			mapper.hasVersion() == true
	} // }}}

	def "Should be capable of retain the version alongside the other fields"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('partitionKey', 'one', FieldType.PARTITION_KEY)
				with('sortKey', 2, FieldType.SORT_KEY)
				with('dataField', true)
				with('v', 1, FieldType.VERSION)
			}
		expect:
			mapper.version() == version
			mapper.storer() == (storer + version)

		when: 'when the version is incremented'
			mapper.incrementVersion()
		then:
			mapper.storer() == (storer + incrementedVersion)

		where:
			storer = [
				partitionKey: fromS('one'),
				sortKey:      fromN('2'),
				dataField:    fromBool(true)
			]
		and:
			version = [ v: fromN('1') ]
		and:
			incrementedVersion = [ v: fromN('2') ]
	} // }}}

}
// vim: fdm=marker
