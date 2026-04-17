package it.grational.storage.dynamodb

import spock.lang.Specification
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class IterableOverloadUSpec extends Specification {

	def "Should be able to use Iterable of Storables in with() method"() {
		given:
			DynamoMapper mapper = new DynamoMapper()
			List<DynamoMap> storables = [
				new DynamoMap([id: '1', name: 'Alice']),
				new DynamoMap([id: '2', name: 'Bob'])
			]

		when:
			// This currently wouldn't compile/work without casting to array
			mapper.withItems('users', true, storables)

		then:
			def stored = mapper.storer(false)
			stored.containsKey('users')
			stored.get('users').type() == AttributeValue.Type.L
			stored.get('users').l().size() == 2
	}

	def "Should be able to use Iterable of DbMappers in with() method"() {
		given:
			DynamoMapper mapper = new DynamoMapper()
			List<DynamoMapper> mappers = [
				new DynamoMapper().with('id', '1'),
				new DynamoMapper().with('id', '2')
			]

		when:
			mapper.withMappers('items', true, mappers)

		then:
			def stored = mapper.storer(false)
			stored.containsKey('items')
			stored.get('items').l().size() == 2
	}

	def "Should work with CompileStatic (Java-like usage)"() {
		given:
			DynamoDbMapper mapper = new DynamoMapper()
			List<Storable> storables = [
				new DynamoMap([id: '1']),
				new DynamoMap([id: '2'])
			]

		when:
			mapper.withItems('items', true, storables)

		then:
			mapper.storer(false).get('items').l().size() == 2
	}
}
