package it.grational.storage.dynamodb

import spock.lang.Specification
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class JavaCompatibilityUSpec extends Specification {

	def "Should support Java-friendly mapper chaining"() {
		when:
			DynamoDbMapper mapper = JavaCompatibilityFixture.chainMapper (
				new DynamoMapper()
			)
			Map<String, AttributeValue> stored = mapper.storer(false)

		then:
			stored.id.s() == 'user-1'
			stored.name.s() == 'Ada'
			stored.age.n() == '42'
			stored.active.bool()
			stored.nickname.nul()
	}

	def "Should support Java collections for nested storables and mappers"() {
		given:
			List<JavaCompatibilityFixture.JavaBean> beans = [
				new JavaCompatibilityFixture.JavaBean('1', 'Ada', 10),
				new JavaCompatibilityFixture.JavaBean('2', 'Grace', 20)
			]
			List<DynamoMapper> mappers = [
				new DynamoMapper().with('id', 'mapper-1'),
				new DynamoMapper().with('id', 'mapper-2')
			]

		when:
			DynamoDbMapper mapper = JavaCompatibilityFixture.addCollections (
				new DynamoMapper(),
				beans,
				mappers
			)
			Map<String, AttributeValue> stored = mapper.storer(false)

		then:
			stored.beans.l().size() == 2
			stored.mappers.l().size() == 2
	}

	def "Should support Java default methods for single nested storables"() {
		given:
			def bean = new JavaCompatibilityFixture.JavaBean('1', 'Ada', 10)

		when:
			DynamoDbMapper mapper = JavaCompatibilityFixture.addNestedItem (
				new DynamoMapper(),
				bean
			)

		then:
			mapper.storer().bean.m().id.s() == '1'
			mapper.builder().bean.name == 'Ada'
	}

	def "Should expose DynamoMap as a standard Java Map"() {
		given:
			DynamoMap dynamoMap = new DynamoMap([id: '1'])

		when:
			Map<String, Object> map = JavaCompatibilityFixture.asMap(dynamoMap)

		then:
			map.id == '1'
			map.fromJava == true
	}

	def "Should support Java-friendly KeyFilter builder"() {
		when:
			KeyFilter key = JavaCompatibilityFixture.keyFilter()

		then:
			key.composite()
			key.toMap().id.s() == 'user-1'
			key.toMap().createdAt.n() == '10'
	}

	def "Should instantiate JavaBeans with converted numeric setter values"() {
		given:
			DynamoDb db = new DynamoDb()

		when:
			def bean = db.instantiate (
				JavaCompatibilityFixture.JavaBean,
				[
					id: '1',
					name: 'Ada',
					score: 42G
				]
			)

		then:
			bean.id == '1'
			bean.name == 'Ada'
			bean.score == 42
	}

}
