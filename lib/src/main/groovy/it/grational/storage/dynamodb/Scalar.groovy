package it.grational.storage.dynamodb

import groovy.transform.TupleConstructor
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

@TupleConstructor
class Scalar {
	String name
	ScalarAttributeType type

	static of(String name, ScalarAttributeType type) {
		new Scalar(name, type)
	}
}
