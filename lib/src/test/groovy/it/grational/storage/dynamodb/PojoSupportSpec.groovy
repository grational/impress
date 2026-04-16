package it.grational.storage.dynamodb

import spock.lang.Specification
import it.grational.storage.DbMapper
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class PojoSupportSpec extends Specification {

	// A class that mimics a standard Java Bean (No-arg constructor + setters)
	// We explicitly add a constructor to prevent Groovy from adding the default Map-constructor
	static class JavaBean implements DynamoStorable {
		String id
		String name
		
		JavaBean() {} 
		
		@Override
		DynamoDbMapper impress(DynamoDbMapper ddm, boolean versioned) {
			ddm.with('id', id, FieldType.PARTITION_KEY)
			ddm.with('name', name)
			return ddm
		}
	}

	// A class that mimics a Java Record (Immutable, constructor with all fields)
	static record JavaRecord(String id, String name) implements DynamoStorable {
		@Override
		DynamoDbMapper impress(DynamoDbMapper ddm, boolean versioned) {
			ddm.with('id', id() as String, FieldType.PARTITION_KEY)
			ddm.with('name', name() as String)
			return ddm
		}
	}

	def "Should be able to instantiate JavaBean using smart instantiation"() {
		given:
			Map<String, Object> data = [id: '123', name: 'John']
			DynamoDb db = new DynamoDb()
		
		when:
			// Using private method via reflection for unit testing the logic
			def instance = db.instantiate(JavaBean, data)
			
		then:
			instance instanceof JavaBean
			instance.id == '123'
			instance.name == 'John'
	}

	def "Should be able to instantiate JavaRecord using smart instantiation"() {
		given:
			Map<String, Object> data = [id: '123', name: 'John']
			DynamoDb db = new DynamoDb()

		when:
			def instance = db.instantiate(JavaRecord, data)

		then:
			instance instanceof JavaRecord
			instance.id() == '123'
			instance.name() == 'John'
	}
}
