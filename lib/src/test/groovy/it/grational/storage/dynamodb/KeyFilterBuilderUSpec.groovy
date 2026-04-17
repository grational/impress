package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import software.amazon.awssdk.core.SdkBytes
import static software.amazon.awssdk.core.SdkBytes.*
import static it.grational.storage.dynamodb.DynamoFilter.*

class KeyFilterBuilderUSpec extends Specification {

	def "Should build a simple partition key filter"() {
		when:
			KeyFilter km = KeyFilter.partition('userId', 'user123').build()

		then:
			km.composite() == false
			km.toMap() == [userId: fromS('user123')]
	}

	def "Should build a composite key filter with string values"() {
		when:
			KeyFilter km = KeyFilter.partition('userId', 'user123')
				.sort('timestamp', '2025-01-01')
				.build()

		then:
			km.composite() == true
			km.toMap() == [userId: fromS('user123'), timestamp: fromS('2025-01-01')]
	}

	def "Should build a composite key filter with numeric values"() {
		when:
			KeyFilter km = KeyFilter.partition('gameId', 12345)
				.sort('score', 99.9)
				.build()

		then:
			km.composite() == true
			km.toMap() == [gameId: fromN('12345'), score: fromN('99.9')]
	}

	def "Should build a composite key filter with binary values"() {
		given:
			SdkBytes pkBytes = fromUtf8String('pk-binary')
			SdkBytes skBytes = fromUtf8String('sk-binary')

		when:
			KeyFilter km = KeyFilter.partition('pk', pkBytes)
				.sort('sk', skBytes)
				.build()

		then:
			km.composite() == true
			km.toMap() == [pk: fromB(pkBytes), sk: fromB(skBytes)]
	}

	def "Should build a composite key filter using a DynamoFilter for the sort key"() {
		when:
			KeyFilter km = KeyFilter.partition('userId', 'user123')
				.sort(greater('timestamp', 1621234567))
				.build()

		then:
			km.composite() == true
			km.condition() == "#userId = :userId AND #attr_timestamp > :val_timestamp"
			km.conditionNames().containsKey('#attr_timestamp')
	}

	def "Should be compatible with Java-like usage (explicit types)"() {
		given:
			// Simulating Java syntax
			KeyFilter.Builder builder = KeyFilter.partition("id", "val")
			builder.sort("ts", 123)
			KeyFilter km = builder.build()

		expect:
			km != null
			km.composite() == true
	}

	def "Should support all partition key overloads"() {
		expect:
			KeyFilter.partition('k', 'v').build().toMap().get('k').s() == 'v'
			KeyFilter.partition('k', 123).build().toMap().get('k').n() == '123'
			KeyFilter.partition('k', 'bin'.bytes).build().toMap().get('k').b() != null
			KeyFilter.partition('k', fromUtf8String('bin')).build().toMap().get('k').b() != null
	}

	def "Should build immutable key filters from mutable builders"() {
		given:
			KeyFilter.Builder builder = KeyFilter.partition('id', 'first')

		when:
			KeyFilter first = builder.build()
			builder.partition('id', 'second')
			KeyFilter second = builder.build()

		then:
			first.toMap().id.s() == 'first'
			second.toMap().id.s() == 'second'
	}

}
