package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

class IndexUSpec extends Specification {

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

	def "Should be able to store an index object composed by one or two scalars"() {
		when:
			def index = Index.of (
				partition,
				sort,
				name
			)

		then:
			index.partition == partition
			index.name == name

			if ( sort )
				index.sort == sort
			else
				index.sort == Optional.empty()

		where:
			partition           | sort                | name
			scalars.part.string | null                | 'p-string-index'
			scalars.part.number | null                | 'p-number-index'
			scalars.part.binary | null                | 'p-binary-index'
			// composite string + *
			scalars.part.string | scalars.sort.string | 'p-string-s-string-index'
			scalars.part.string | scalars.sort.number | 'p-string-s-number-index'
			scalars.part.string | scalars.sort.binary | 'p-string-s-binary-index'
			// composite number + *
			scalars.part.number | scalars.sort.string | 'p-number-s-string-index'
			scalars.part.number | scalars.sort.number | 'p-number-s-number-index'
			scalars.part.number | scalars.sort.binary | 'p-number-s-binary-index'
			// composite binary + *
			scalars.part.binary | scalars.sort.string | 'p-binary-s-string-index'
			scalars.part.binary | scalars.sort.number | 'p-binary-s-number-index'
			scalars.part.binary | scalars.sort.binary | 'p-binary-s-binary-index'
	}

}
