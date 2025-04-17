package it.grational.storage

import groovy.transform.CompileStatic

@CompileStatic
interface DbMapper<S,B> {
	DbMapper<S,B> with(String k, String s)
	DbMapper<S,B> with(String k, Number n)
	DbMapper<S,B> with(String k, boolean b)
	DbMapper<S,B> with(String k, DbMapper<S,B> dm, boolean version)
	DbMapper<S,B> with(String k, List<String> ls)
	Map<String,S> storer(boolean version)
	Map<String,B> builder(boolean version)
}
