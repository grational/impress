package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import it.grational.storage.Storable

class DynamoMapperUSpec extends Specification {

	def "Should support all the types of attributes"() { // {{{
		when:
			def mapper = new DynamoMapper().tap {
				with('string', 'value')
				with('number', 1)
				with('boolean', true)
				with('stringSet', 'value1', 'value2')
				with('numberSet', 1, 2)
				withNull('nullValue')
			}
		then:
			mapper.storer() == [
				string:    fromS('value'),
				number:    fromN('1'),
				boolean:   fromBool(true),
				stringSet: fromSs(['value1', 'value2']),
				numberSet: fromNs(['1', '2']),
				nullValue: fromNul(true)
			]
	} // }}}

	def "Should support nested mappers"() { // {{{
		given:
			def nested = new DynamoMapper().tap {
				with('string', 'value')
				with('number', 1)
				with('boolean', true)
				with('stringSet', 'value1', 'value2')
				with('numberSet', 1, 2)
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

	def "Should support lists of objects"() { // {{{
		given:
			def object1 = Stub(Storable) {
				impress(_, _) >> { args ->
					args[0]
					.with('name', 'Object 1')
					.with('value', 100)
				}
			}
			def object2 = Stub(Storable) {
				impress(_, _) >> { args ->
					args[0]
					.with('name', 'Object 2')
					.with('value', 200)
				}
			}
		
		when:
			def mapper = new DynamoMapper().tap {
				with('objects', true, object1, object2)
			}
			
		then:
			mapper.storer() == [
				objects: fromL([
					fromM([
						name: fromS('Object 1'),
						value: fromN('100')
					]),
					fromM([
						name: fromS('Object 2'),
						value: fromN('200')
					])
				])
			]
	} // }}}
	
	def "Should handle null or empty lists of objects"() { // {{{
		given:
			boolean versioned = true
			Storable[] storables = new Storable[0]

		when:
			def mapper = new DynamoMapper().tap {
				with('objects', versioned, storables)
			}

		then:
			mapper.storer() == [:]
	} // }}}
	
	def "Should convert list of objects in a single builder"() { // {{{
		given:
			boolean versioned = true
		and:
			def object1 = Stub(Storable) {
				impress(_, _) >> { args ->
					args[0]
					.with('name', 'Object 1')
					.with('value', 100)
				}
			}
			def object2 = Stub(Storable) {
				impress(_, _) >> { args ->
					args[0]
					.with('name', 'Object 2')
					.with('value', 200)
				}
			}
			
		when:
			def mapper = new DynamoMapper().tap {
				with('objects', versioned, object1, object2)
			}
			def builder = mapper.builder()
			def objects = builder.objects
			
		then:
			objects.size() == 2
			objects[0].name == 'Object 1'
			objects[0].value == 100
			objects[1].name == 'Object 2'
			objects[1].value == 200
	} // }}}
		
	def "Should support adding a Storable directly to a field"() { // {{{
		given:
			def storable = Stub(Storable) {
				impress(_, _) >> { args ->
					args[0]
					.with('name', 'Test Object')
					.with('value', 100)
				}
			}
		
		when:
			def mapper = new DynamoMapper().tap {
				with('object', storable)
			}
			
		then:
			mapper.storer() == [
				object: fromM([
					name: fromS('Test Object'),
					value: fromN('100')
				])
			]
	} // }}}
	
	def "Should correctly generate version condition expressions"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('v', 1, FieldType.VERSION)
			}
			
		when:
			def condition = mapper.versionCondition()
			
		then:
			condition == "attribute_not_exists(#v) OR #v = :current_v"
			
		when:
			condition = mapper.versionCondition(false)
			
		then:
			condition == "attribute_not_exists(#v) OR #v = :v"
	} // }}}
	
	def "Should generate version names and values maps"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('v', 1, FieldType.VERSION)
			}
			
		when:
			def names = mapper.versionNames()
			def values = mapper.versionValues()
			def nonCurrentValues = mapper.versionValues(false)
			
		then:
			names == ['#v': 'v']
			values == [':current_v': fromN('1')]
			nonCurrentValues == [':v': fromN('1')]
			
		when:
			mapper = new DynamoMapper()
			
		then:
			mapper.versionNames() == [:]
			mapper.versionValues() == [:]
			mapper.versionValues(false) == [:]
	} // }}}
	
	def "Should generate update expressions"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('field1', 'value1')
				with('field2', 2)
				with('field3', true)
			}
		
		when:
			def updateExpr = mapper.updateExpression()
			
		then:
			updateExpr.startsWith("SET ")
			updateExpr.contains("#field1 = :field1")
			updateExpr.contains("#field2 = :field2")
			updateExpr.contains("#field3 = :field3")
	} // }}}
	
	def "Should generate expression names and values"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('field1', 'value1')
				with('field2', 2)
			}
			
		when:
			def names = mapper.expressionNames()
			def values = mapper.expressionValues()
			
		then:
			names == ['#field1': 'field1', '#field2': 'field2']
			values == [':field1': fromS('value1'), ':field2': fromN('2')]
			
		when:
			def customNames = ['#other': 'other']
			def customValues = [':other': fromS('otherValue')]
			names = mapper.expressionNames(customNames)
			values = mapper.expressionValues(customValues)
			
		then:
			names == ['#other': 'other', '#field1': 'field1', '#field2': 'field2']
			values == [':other': fromS('otherValue'), ':field1': fromS('value1'), ':field2': fromN('2')]
	} // }}}
	
	def "Should handle special characters in field names"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('field-1', 'value1')
				with('field.2', 2)
			}
			
		when:
			def names = mapper.expressionNames()
			def values = mapper.expressionValues()
			
		then:
			names == ['#field1': 'field-1', '#field2': 'field.2']
			values == [':field1': fromS('value1'), ':field2': fromN('2')]
	} // }}}
	
	def "Should support removing attributes"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				remove('attribute1', 'attribute2', 'attribute3')
			}
			
		when:
			def updateExpr = mapper.updateExpression()
			def names = mapper.expressionNames()
			
		then:
			updateExpr == "REMOVE #attribute1, #attribute2, #attribute3"
			names == [
				'#attribute1': 'attribute1',
				'#attribute2': 'attribute2', 
				'#attribute3': 'attribute3'
			]
	} // }}}
	
	def "Should combine SET and REMOVE in update expressions"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				with('field1', 'newValue')
				with('field2', 42)
				remove('oldField1', 'oldField2')
			}
			
		when:
			def updateExpr = mapper.updateExpression()
			def names = mapper.expressionNames()
			def values = mapper.expressionValues()
			
		then:
			updateExpr.contains("SET #field1 = :field1, #field2 = :field2")
			updateExpr.contains("REMOVE #oldField1, #oldField2")
			names == [
				'#field1': 'field1',
				'#field2': 'field2',
				'#oldField1': 'oldField1',
				'#oldField2': 'oldField2'
			]
			values == [
				':field1': fromS('newValue'),
				':field2': fromN('42')
			]
	} // }}}
	
	def "Should handle null and empty attribute names in remove"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				remove('validField', null, '', '  ', 'anotherValidField')
			}
			
		when:
			def updateExpr = mapper.updateExpression()
			def names = mapper.expressionNames()
			
		then:
			updateExpr == "REMOVE #validField, #anotherValidField"
			names == [
				'#validField': 'validField',
				'#anotherValidField': 'anotherValidField'
			]
	} // }}}
	
	def "Should handle special characters in remove attribute names"() { // {{{
		given:
			def mapper = new DynamoMapper().tap {
				remove('field-with-dashes', 'field.with.dots')
			}
			
		when:
			def updateExpr = mapper.updateExpression()
			def names = mapper.expressionNames()
			
		then:
			updateExpr == "REMOVE #fieldwithdashes, #fieldwithdots"
			names == [
				'#fieldwithdashes': 'field-with-dashes',
				'#fieldwithdots': 'field.with.dots'
			]
	} // }}}
}
// vim: fdm=marker
