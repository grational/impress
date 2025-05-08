package it.grational.storage.dynamodb

// imports {{{
import groovy.transform.ToString
import groovy.transform.CompileStatic
import it.grational.storage.DbMapper
import it.grational.storage.Storable
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import static it.grational.storage.dynamodb.FieldType.*
// }}}

@ToString (
	includePackage = false,
	includeFields = true,
	includeNames = true
)
@CompileStatic
class DynamoMapper implements DbMapper<AttributeValue,Object> {
	private Map<String, AttributeValue> map = [:]
	private Tuple2<String, AttributeValue> pk
	private Tuple2<String, AttributeValue> sk
	private Tuple2<String, AttributeValue> vf

	DynamoMapper(Map<String, AttributeValue> m = [:]) {
		map.putAll(m)
	}

	// DbMapper implementation {{{
	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		String s
	) {
		if (s == null) return this
		with(k, s, STANDARD)
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		Number n
	) {
		if (n == null) return this
		with(k, n, STANDARD)
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		boolean b
	) {
		map[k] = fromBool(b)
		return this
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		DbMapper<AttributeValue,Object> dm,
		boolean version = true
	) {
		if (dm == null) return this
		map[k] = fromM(dm.storer(version))
		return this
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		String... ls
	) {
		List<String> nnls = ls?.findAll { it != null }
		if (!nnls) return this
		map[k] = fromSs(nnls)
		return this
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		Number... ln
	) {
		List<String> nnln = ln?.findResults { Number n ->
			n?.toString()
		} as List<String>
		if (!nnln) return this
		map[k] = fromNs(nnln)
		return this
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		boolean versioned,
		Storable<AttributeValue,Object>... ls
	) {
		with (
			k,
			versioned,
			ls?.findResults { Storable<AttributeValue,Object> st ->
				st?.impress(new DynamoMapper(), versioned)
			}.toArray(new DbMapper[0])
		)
	}

	@Override
	DbMapper<AttributeValue,Object> with (
		String k,
		boolean versioned,
		DbMapper<AttributeValue,Object>... ldm
	) {
		List<DbMapper<AttributeValue,Object>> nnldm =
			ldm?.findAll { it != null }
		if (!nnldm) return this

		map[k] = fromL (
			nnldm.collect { DbMapper<AttributeValue,Object> dm ->
				fromM(dm.storer(versioned))
			}
		)

		return this
	}

	@Override
	Map<String, AttributeValue> storer(boolean version = true) {
		return aggregate(version)
	}

	@Override
	Map<String, Object> builder(boolean version = true) {
		aggregate(version)
		.collectEntries { String k, AttributeValue v ->
			[ (k): fromAttribute(v) ]
		}
	}

	private Map<String, AttributeValue> aggregate(boolean version) {
		Map<String, AttributeValue> result = fromT2(pk) + fromT2(sk) + map
		return ( version ) ? ( fromT2(vf) + result ) : result
	}
	// }}}

	// Additional methods {{{

	boolean hasKey() { // {{{
		return ( pk != null )
	} // }}}

	Map<String, AttributeValue> key() { // {{{
		return ( hasKey() )
			? ( fromT2(pk) + fromT2(sk) )
			: [:]
	} // }}}

	boolean hasVersion() { // {{{
		return ( vf != null )
	} // }}}

	Map<String, AttributeValue> version() { // {{{
		return hasVersion()
			? fromT2(vf)
			: [:]
	} // }}}

	private Map<String, AttributeValue> fromT2 ( // {{{
		Tuple2<String, AttributeValue> t
	) {
		return (t) ? [ (t.V1): t.V2 ] : [:]
	} // }}}

	void incrementVersion() { // {{{
		if ( !hasVersion() )
			return
		map << [ (vf.V1): increment(vf.V2) ]
	} // }}}

	private AttributeValue increment(AttributeValue av) { // {{{
		fromN (
			( currentVersion(av) + 1 ).toString()
		)
	} // }}}

	private int currentVersion(AttributeValue av) { // {{{
		av.n().toInteger() ?: 0
	} // }}}

	DbMapper<AttributeValue,Object> with ( // {{{
		String k,
		String s,
		FieldType t
	) {
		if (s == null) return this

		switch(t) {
			case PARTITION_KEY:
				pk = new Tuple2(k, fromS(s)); break
			case SORT_KEY:
				sk = new Tuple2(k, fromS(s)); break
			case VERSION:
				vf = new Tuple2(k, fromN(s)); break
			default:
				map[k] = fromS(s)
		}
		return this
	} // }}}

	DbMapper<AttributeValue,Object> with ( // {{{
		String k,
		Number n,
		FieldType t
	) {
		if (n == null) return this

		switch(t) {
			case PARTITION_KEY:
				pk = new Tuple2(k, fromN(n.toString()))
				break
			case SORT_KEY:
				sk = new Tuple2(k, fromN(n.toString()))
				break
			case VERSION:
				vf = new Tuple2(k, fromN(n.toString()))
				break
			default:
				map[k] = fromN(n.toString())
		}
		return this
	} // }}}

	DbMapper<AttributeValue,Object> with ( // {{{
		String k,
		Storable<AttributeValue,Object> st,
		DbMapper<AttributeValue,Object> dm = new DynamoMapper(),
		boolean versioned = true
	) {
		if (st == null) return this

		dm = st.impress(dm, versioned)
		with(k, dm, versioned)
	} // }}}

	DbMapper<AttributeValue,Object> withNull(String k) {
		map[k] = AttributeValue.builder().nul(true).build()
		return this
	}

	String versionCondition(boolean current = true) { // {{{
		String.join(" ",
			"attribute_not_exists(${versionName()})",
			'OR',
			"${versionName()} = ${versionValue(current)}"
		)
	} // }}}

	private String versionName() { // {{{
		"#${safe(vf.V1)}"
	} // }}}

	private String versionValue(boolean current) { // {{{
		":${ (current) ? 'current_' : ''}${safe(vf.V1)}"
	} // }}}

	Map<String,String> versionNames() { // {{{
		return hasVersion() 
			? [ (versionName()): vf.V1 ]
			: [:]
	} // }}}

	Map<String,AttributeValue> versionValues (
		boolean current = true
	) { // {{{
		return hasVersion() 
			? [ (versionValue(current)): vf.V2 ]
			: [:]
	} // }}}

	String updateExpression() { // {{{
		"SET ${setList().join(', ')}"
	} // }}}

	private List<GString> setList() { // {{{
		map.collect { k, v ->
			"#${safe(k)} = :${safe(k)}"
		}
	} // }}}

	Map<String,String> expressionNames ( // {{{
		Map<String,String> others = [:]
	) {
		map.collectEntries(others) { String k, AttributeValue v ->
			[ ("#${safe(k)}" as String): k ]
		} as Map<String,String>
	} // }}}

	Map<String,AttributeValue> expressionValues ( // {{{
		Map<String,AttributeValue> others = [:]
	) {
		map.collectEntries(others) { String k, AttributeValue v ->
			[ (":${safe(k)}" as String): v ]
		} as Map<String,AttributeValue>
	} // }}}

	private String safe(String name) { // {{{
		name.replaceAll(/[^a-zA-Z0-9_]/,'')
	} // }}}

	// }}}

	// Private methods {{{
	private Object fromAttribute(AttributeValue av) { // {{{
		switch (av.type()) {
			case Type.NUL:
				return null
			case Type.S:
				return av.s()
			case Type.N:
				return av.n().asType(BigDecimal)
			case Type.BOOL:
				return av.bool()
			case Type.L:
				return av.l().collect {
					fromAttribute(it)
				}
			case Type.M:
				return av.m().collectEntries { k, v ->
					[ k, fromAttribute(v) ]
				}
			case Type.SS:
				return av.ss() as ArrayList
			case Type.NS:
				return av.ns()*.asType(BigDecimal)
			default:
				throw new IllegalArgumentException (
					"Unsupported Attribute: ${av.type()}"
				)
		}
	} // }}}
	// }}}

}
// vim: fdm=marker
