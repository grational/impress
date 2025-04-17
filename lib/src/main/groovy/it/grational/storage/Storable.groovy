package it.grational.storage

import groovy.transform.CompileStatic

@CompileStatic
interface Storable<S,B> {
	DbMapper<S,B> impress (
		DbMapper<S,B> mapper,
		boolean versioned
	)
}
