package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import spock.lang.Specification

class DynamoMapJavaCompatibilitySpec extends Specification {

	def "DynamoMap should be assignable to a Map variable (Java-like compatibility)"() {
		given:
			DynamoMap dynamoMap = new DynamoMap([foo: 'bar'])

		when:
			// This line would fail at compile time if DynamoMap 
			// didn't explicitly implement Map
			Map<String, Object> map = dynamoMap

		then:
			map.get('foo') == 'bar'
			map.size() == 1
	}

	def "DynamoMap should work with standard Map methods"() {
		given:
			Map<String, Object> map = new DynamoMap()

		when:
			map.put('key', 'value')
			map.put('number', 123)

		then:
			map.get('key') == 'value'
			map.get('number') == 123
			map.containsKey('key')
			map.size() == 2
	}

	def "DynamoMap should be able to be passed to methods expecting a Map"() {
		given:
			DynamoMap dynamoMap = new DynamoMap([a: 1, b: 2])

		when:
			int size = takeMap(dynamoMap)

		then:
			size == 2
	}

	@CompileStatic
	private int takeMap(Map<String, Object> map) {
		return map.size()
	}

}
