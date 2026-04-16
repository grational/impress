package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

@CompileStatic
@TupleConstructor
class Scalar {
	String name
	ScalarAttributeType type

	static Scalar of (
		String name,
		ScalarAttributeType type = ScalarAttributeType.S
	) {
		new Scalar(name, type)
	}
}
