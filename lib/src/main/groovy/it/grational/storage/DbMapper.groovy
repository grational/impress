package it.grational.storage

import groovy.transform.CompileStatic

@CompileStatic
interface DbMapper<S,B> {
	DbMapper<S,B> with(String k, String s)
	DbMapper<S,B> with(String k, Number n)
	DbMapper<S,B> with(String k, boolean b)
	DbMapper<S,B> with(String k, DbMapper<S,B> dm, boolean v)
	DbMapper<S,B> with(String k, String... ls)
	DbMapper<S,B> with(String k, Number... ln)
	DbMapper<S,B> with(String k, boolean v, Storable<S,B>... ast)
	DbMapper<S,B> with(String k, boolean v, DbMapper<S,B>... adm)
	default DbMapper<S,B> withItems (
		String k,
		boolean v,
		Iterable<? extends Storable<S,B>> ast
	) {
		List<Storable<S,B>> items = ast?.toList() ?: []
		return with(k, v, items.toArray(new Storable[0]) as Storable<S,B>[])
	}
	default DbMapper<S,B> withMappers (
		String k,
		boolean v,
		Collection<? extends DbMapper<S,B>> adm
	) {
		List<DbMapper<S,B>> mappers = adm?.toList() ?: []
		return with(k, v, mappers.toArray(new DbMapper[0]) as DbMapper<S,B>[])
	}
	Map<String,S> storer(boolean version)
	default Map<String,S> storer() {
		return storer(true)
	}
	Map<String,B> builder(boolean version)
	default Map<String,B> builder() {
		return builder(true)
	}
}
