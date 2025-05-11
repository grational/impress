package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

class KeysUSpec extends Specification {

	@Shared
	Map<String,Scalar> scalars = [
		part: [
			string: Scalar.of('p-string', ScalarAttributeType.S),
			number: Scalar.of('p-number', ScalarAttributeType.N),
			binary: Scalar.of('p-binary', ScalarAttributeType.B)
		],
		sort: [
			string: Scalar.of('s-string', ScalarAttributeType.S),
			number: Scalar.of('s-number', ScalarAttributeType.N),
			binary: Scalar.of('s-binary', ScalarAttributeType.B)
		]
	]

	def "Should be able to store an keys object composed by one or two scalars"() {
		when:
			def keys = Keys.of (
				partition,
				sort
			)

		then:
			if ( sort )
				keys.sort == sort
			else
				keys.sort == Optional.empty()

			and:
				keys.attributes() == [ partition, sort ].grep()

		where:
			partition           | sort
			scalars.part.string | null
			scalars.part.number | null
			scalars.part.binary | null
			// composite string + *
			scalars.part.string | scalars.sort.string
			scalars.part.string | scalars.sort.number
			scalars.part.string | scalars.sort.binary
			// composite number + *
			scalars.part.number | scalars.sort.string
			scalars.part.number | scalars.sort.number
			scalars.part.number | scalars.sort.binary
			// composite binary + *
			scalars.part.binary | scalars.sort.string
			scalars.part.binary | scalars.sort.number
			scalars.part.binary | scalars.sort.binary
	}

}
