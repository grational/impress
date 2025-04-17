package it.grational.storage.dynamodb

import groovy.transform.ToString
import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*

@ToString (
	includeNames = true,
	includeFields = true,
	includePackage = false
)
@CompileStatic
class DynamoKey {
	private final Map<String, AttributeValue> map = [:]

	DynamoKey(Map<String, AttributeValue> key) {
		if ( key.size() !in [1, 2] )
			throw new IllegalArgumentException (
				"Invalid key size: ${key.size()}"
			)
		if ( key.any { k, v -> v.type() !in [ Type.S, Type.N ] } )
			throw new IllegalArgumentException (
				"Unsupported key types: ${key}"
			)
		map = key
	}

	DynamoKey(String k, String v) {
		map << [ (k): fromS(v) ]
	}

	DynamoKey(String k, Number v) {
		map << [ (k): fromN(v.toString()) ]
	}

	DynamoKey(String pk, String pv, String sk, String sv) {
		map << [ (pk): fromS(pv), (sk): fromS(sv) ]
	}

	DynamoKey(String pk, String pv, String sk, Number sv) {
		map << [ (pk): fromS(pv), (sk): fromN(sv.toString()) ]
	}

	DynamoKey(String pk, Number pv, String sk, String sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromS(sv) ]
	}

	DynamoKey(String pk, Number pv, String sk, Number sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromN(sv.toString()) ]
	}

	Map<String,AttributeValue> toMap() {
		return map
	}

	boolean composite() {
		map.size() > 1
	}

	String condition() {
		map.collect { k, v ->
			"#${safe(k)} = :${safe(k)}"
		}.join(' AND ')
	}

	Map<String, String> conditionNames() {
		map.collectEntries { k, v ->
			[ ("#${safe(k)}" as String): k ]
		}
	}

	Map<String, AttributeValue> conditionValues() {
		map.collectEntries { k, v ->
			[ (":${safe(k)}" as String): v ]
		}
	}

	private String safe(String name) {
		name.replaceAll(/[^a-zA-Z0-9_]/,'')
	}

}
