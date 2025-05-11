package it.grational.storage.dynamodb

import groovy.transform.ToString
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import static it.grational.storage.dynamodb.NestedPathProcessor.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import software.amazon.awssdk.core.SdkBytes
import static software.amazon.awssdk.core.SdkBytes.*

@ToString (
	includeNames = true,
	includeFields = true,
	includePackage = false
)
@EqualsAndHashCode
@CompileStatic
class KeyMatch {
	private final Map<String, AttributeValue> map = [:]

	KeyMatch(Map<String, AttributeValue> key) {
		if ( key.size() !in [1, 2] )
			throw new IllegalArgumentException (
				"Invalid key size: ${key.size()}"
			)
		if ( key.any { k, v -> v.type() !in [ Type.S, Type.N, Type.B ] } )
			throw new IllegalArgumentException (
				"Unsupported key types: ${key}"
			)
		map = key
	}

	KeyMatch(String k, String v) {
		map << [ (k): fromS(v) ]
	}

	KeyMatch(String k, Number v) {
		map << [ (k): fromN(v.toString()) ]
	}

	KeyMatch(String k, byte[] v) {
		this(k, fromByteArray(v))
	}
	KeyMatch(String k, SdkBytes v) {
		map << [ (k): fromB(v) ]
	}

	KeyMatch(String pk, String pv, String sk, String sv) {
		map << [ (pk): fromS(pv), (sk): fromS(sv) ]
	}

	KeyMatch(String pk, String pv, String sk, Number sv) {
		map << [ (pk): fromS(pv), (sk): fromN(sv.toString()) ]
	}

	KeyMatch(String pk, String pv, String sk, byte[] sv) {
		this(pk, pv, sk, fromByteArray(sv))
	}
	KeyMatch(String pk, String pv, String sk, SdkBytes sv) {
		map << [ (pk): fromS(pv), (sk): fromB(sv) ]
	}

	KeyMatch(String pk, Number pv, String sk, String sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromS(sv) ]
	}

	KeyMatch(String pk, Number pv, String sk, Number sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromN(sv.toString()) ]
	}

	KeyMatch(String pk, Number pv, String sk, byte[] sv) {
		this(pk, pv, sk, fromByteArray(sv))
	}
	KeyMatch(String pk, Number pv, String sk, SdkBytes sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromB(sv) ]
	}

	KeyMatch(String pk, byte[] pv, String sk, String sv) {
		this(pk, fromByteArray(pv), sk, sv)
	}
	KeyMatch(String pk, SdkBytes pv, String sk, String sv) {
		map << [ (pk): fromB(pv), (sk): fromS(sv) ]
	}

	KeyMatch(String pk, byte[] pv, String sk, Number sv) {
		this(pk, fromByteArray(pv), sk, sv)
	}
	KeyMatch(String pk, SdkBytes pv, String sk, Number sv) {
		map << [ (pk): fromB(pv), (sk): fromN(sv.toString()) ]
	}

	KeyMatch(String pk, byte[] pv, String sk, byte[] sv) {
		this(pk, fromByteArray(pv), sk, fromByteArray(sv))
	}
	KeyMatch(String pk, SdkBytes pv, String sk, SdkBytes sv) {
		map << [ (pk): fromB(pv), (sk): fromB(sv) ]
	}

	Map<String,AttributeValue> toMap() {
		return map
	}

	boolean composite() {
		map.size() > 1
	}

	KeyMatch partition() {
		return ( composite() )
			? new KeyMatch(map.take(1))
			: this
	}

	Optional<KeyMatch> sort() {
		return Optional.ofNullable (
			composite()
				? new KeyMatch(map.drop(1))
				: null
		)
	}

	String condition() {
		map.collect { k, v ->
			PathResult processed = processForKey(k)
			"${processed.nameRef} = :${safeValueName(k)}"
		}.join(' AND ')
	}

	Map<String, String> conditionNames() {
		Map<String,String> result = [:]
		map.each { k, v ->
			PathResult processed = processForKey(k)
			result.putAll(processed.nameMap)
		}
		return result
	}

	Map<String, AttributeValue> conditionValues() {
		map.collectEntries { k, v ->
			[ (":${safeValueName(k)}" as String): v ]
		}
	}

}
