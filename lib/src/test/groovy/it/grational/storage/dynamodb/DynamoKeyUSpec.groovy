package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*

class DynamoKeyUSpec extends Specification {

	def "Should return a valid dynamo key given each supported type"() {
		when:
			DynamoKey dk = new DynamoKey(key, value)

		then:
			noExceptionThrown()
		and:
			dk.composite() == false
		and:
			dk.toMap() == expected

		where:
			key      | value   || expected
			'string' | 'value' || [string: fromS('value')]
			'number' | 1       || [number: fromN('1')]
	}

	def "Should return a valid dynamo key also when a combined key is used"() {
		when:
			DynamoKey dk = new DynamoKey(pk, pv, sk, sv)

		then:
			noExceptionThrown()
		and:
			dk.composite() == true
		and:
			dk.toMap() == expected

		where:
			pk     | pv       | sk     | sv       || expected
			'part' | 'pvalue' | 'sort' | 'svalue' || [ part: fromS('pvalue'), sort: fromS('svalue') ]
			'part' | 'pvalue' | 'sort' | 2        || [ part: fromS('pvalue'), sort: fromN('2')      ]
			'part' | 1        | 'sort' | 2        || [ part: fromN('1'),      sort: fromN('2')      ]
			'part' | 1        | 'sort' | 'svalue' || [ part: fromN('1'),      sort: fromS('svalue') ]
	}

	def "Should be able to return the key condition expression and the relative placeholders"() {
		when:
			DynamoKey dk = new DynamoKey('string', 'value')

		then:
			dk.composite() == false

		and:
			dk.condition() == "#string = :string"
		and:
			dk.conditionNames() == [ '#string': 'string' ]
		and:
			dk.conditionValues() == [':string': fromS('value')]
	}

	def "Should be able to return the key condition expressions for composite keys"() {
		when:
			DynamoKey dk = new DynamoKey (
				'string', 'value',
				'number', 1
			)

		then:
			dk.composite() == true

		and:
			dk.condition() == "#string = :string AND #number = :number"
		and:
			dk.conditionNames() == [
				'#string': 'string',
				'#number': 'number'
			]
		and:
			dk.conditionValues() == [
				':string': fromS('value'),
				':number': fromN('1'),
			]
	}

	def "Should throw an exception if the key map is invalid"() {
		when:
			new DynamoKey(input)

		then:
			Exception e = thrown(IllegalArgumentException)
		and:
			e.message == exceptionMessage

		where:
			input                                             || exceptionMessage
			[:]                                               || 'Invalid key size: 0'
			[ a: fromS('a'), b: fromS('b'), one: fromN('1') ] || 'Invalid key size: 3'
			[ a: fromBool(true) ]                             || "Unsupported key types: [a:AttributeValue(BOOL=true)]"
	}

	def "Should be able to instantiate it directly with an appropriate map"() {
		when:
			DynamoKey dk = new DynamoKey(input)

		then:
			noExceptionThrown()
		and:
			dk.composite() == composite
		and:
			dk.toMap() == input

		where:
			input                                || composite
			[ string: fromS('value') ]           || false
			[ a: fromS('a'), b: fromS('b') ]     || true
			[ a: fromS('a'), one: fromN('1') ]   || true
			[ one: fromN('1'), b: fromS('b') ]   || true
			[ one: fromN('1'), two: fromN('2') ] || true
	}

}
