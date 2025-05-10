package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

class ScalarUSpec extends Specification {

	def "Should be able to store a couple composed by name and scalar type"() {
		when:
			def scalar = new Scalar(name, type)

		then:
			scalar.name == name
			scalar.type == type

		where:
			name      | type
			'name'    | ScalarAttributeType.S
			'number'  | ScalarAttributeType.N
			'boolean' | ScalarAttributeType.B
	}
}
