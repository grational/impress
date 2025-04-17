package it.grational.storage.dynamodb

import groovy.transform.CompileStatic

@CompileStatic
enum FieldType {
	STANDARD,
	VERSION,
	PARTITION_KEY,
	SORT_KEY,
}
