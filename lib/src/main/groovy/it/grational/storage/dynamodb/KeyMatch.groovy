package it.grational.storage.dynamodb

import groovy.transform.ToString
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import static it.grational.storage.dynamodb.NestedPathProcessor.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import software.amazon.awssdk.core.SdkBytes
import static software.amazon.awssdk.core.SdkBytes.*

/**
 * Represents a key condition for DynamoDB operations.
 *
 * KeyMatch is used to represent key conditions when querying or retrieving items from DynamoDB.
 * It supports both single-key (partition key only) and composite key (partition + sort key) conditions
 * with various data types (string, number, binary).
 *
 * Example usage:
 * <pre>
 * // Simple partition key only
 * KeyMatch simpleKey = new KeyMatch("userId", "user123")
 * // or using static factory method
 * KeyMatch simpleKey = KeyMatch.of("userId", "user123")
 *
 * // Composite key with string + number
 * KeyMatch compositeKey = new KeyMatch("userId", "user123", "timestamp", 1621234567)
 * // or using static factory method
 * KeyMatch compositeKey = KeyMatch.of("userId", "user123", "timestamp", 1621234567)
 *
 * // Access components
 * KeyMatch partOnly = compositeKey.partition() // Only userId=user123
 * Optional<KeyMatch> sortOnly = compositeKey.sort() // Only timestamp=1621234567
 *
 * // Convert to DynamoDB map format
 * Map<String, AttributeValue> keyMap = compositeKey.toMap()
 * </pre>
 */
@ToString (
	includeNames = true,
	includeFields = true,
	includePackage = false
)
@EqualsAndHashCode
@CompileStatic
class KeyMatch {
	/** The internal map of attribute names to AttributeValue objects */
	private final Map<String, AttributeValue> map = [:]

	/**
	 * Constructor that accepts a map of attribute names to AttributeValue objects
	 *
	 * @param key The key map (must contain 1 or 2 entries)
	 * @throws IllegalArgumentException if key size is invalid or contains unsupported types
	 */
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

	/**
	 * Creates a key with a string attribute
	 *
	 * @param k The attribute name
	 * @param v The string value
	 */
	KeyMatch(String k, String v) {
		map << [ (k): fromS(v) ]
	}

	/**
	 * Creates a key with a numeric attribute
	 *
	 * @param k The attribute name
	 * @param v The numeric value
	 */
	KeyMatch(String k, Number v) {
		map << [ (k): fromN(v.toString()) ]
	}

	/**
	 * Creates a key with a binary attribute from byte array
	 *
	 * @param k The attribute name
	 * @param v The binary value as byte array
	 */
	KeyMatch(String k, byte[] v) {
		this(k, fromByteArray(v))
	}

	/**
	 * Creates a key with a binary attribute from SdkBytes
	 *
	 * @param k The attribute name
	 * @param v The binary value as SdkBytes
	 */
	KeyMatch(String k, SdkBytes v) {
		map << [ (k): fromB(v) ]
	}

	/**
	 * Creates a composite key with string partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyMatch(String pk, String pv, String sk, String sv) {
		map << [ (pk): fromS(pv), (sk): fromS(sv) ]
	}

	/**
	 * Creates a composite key with string partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyMatch(String pk, String pv, String sk, Number sv) {
		map << [ (pk): fromS(pv), (sk): fromN(sv.toString()) ]
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 */
	KeyMatch(String pk, String pv, String sk, byte[] sv) {
		this(pk, pv, sk, fromByteArray(sv))
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 */
	KeyMatch(String pk, String pv, String sk, SdkBytes sv) {
		map << [ (pk): fromS(pv), (sk): fromB(sv) ]
	}

	/**
	 * Creates a composite key with numeric partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyMatch(String pk, Number pv, String sk, String sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromS(sv) ]
	}

	/**
	 * Creates a composite key with numeric partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyMatch(String pk, Number pv, String sk, Number sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromN(sv.toString()) ]
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 */
	KeyMatch(String pk, Number pv, String sk, byte[] sv) {
		this(pk, pv, sk, fromByteArray(sv))
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 */
	KeyMatch(String pk, Number pv, String sk, SdkBytes sv) {
		map << [ (pk): fromN(pv.toString()), (sk): fromB(sv) ]
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyMatch(String pk, byte[] pv, String sk, String sv) {
		this(pk, fromByteArray(pv), sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyMatch(String pk, SdkBytes pv, String sk, String sv) {
		map << [ (pk): fromB(pv), (sk): fromS(sv) ]
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyMatch(String pk, byte[] pv, String sk, Number sv) {
		this(pk, fromByteArray(pv), sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyMatch(String pk, SdkBytes pv, String sk, Number sv) {
		map << [ (pk): fromB(pv), (sk): fromN(sv.toString()) ]
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 */
	KeyMatch(String pk, byte[] pv, String sk, byte[] sv) {
		this(pk, fromByteArray(pv), sk, fromByteArray(sv))
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 */
	KeyMatch(String pk, SdkBytes pv, String sk, SdkBytes sv) {
		map << [ (pk): fromB(pv), (sk): fromB(sv) ]
	}

	/**
	 * Converts the key to a DynamoDB attribute value map
	 *
	 * @return A map of attribute names to AttributeValue objects
	 */
	Map<String,AttributeValue> toMap() {
		return map
	}

	/**
	 * Checks if this is a composite key (has both partition and sort keys)
	 *
	 * @return true if the key has both partition and sort components, false if it's a partition-only key
	 */
	boolean composite() {
		map.size() > 1
	}

	/**
	 * Gets a KeyMatch representing only the partition key component
	 *
	 * @return A new KeyMatch containing only the partition key, or this object if it's already a partition-only key
	 */
	KeyMatch partition() {
		return ( composite() )
			? new KeyMatch(map.take(1))
			: this
	}

	/**
	 * Gets an Optional KeyMatch representing only the sort key component
	 *
	 * @return An Optional containing a new KeyMatch with just the sort key, or empty if this is a partition-only key
	 */
	Optional<KeyMatch> sort() {
		return Optional.ofNullable (
			composite()
				? new KeyMatch(map.drop(1))
				: null
		)
	}

	/**
	 * Builds a key condition expression for DynamoDB operations
	 *
	 * @return A string representing the key condition expression
	 */
	String condition() {
		map.collect { k, v ->
			PathResult processed = processForKey(k)
			"${processed.nameRef} = :${safeValueName(k)}"
		}.join(' AND ')
	}

	/**
	 * Gets the expression attribute names map for this key condition
	 *
	 * @return A map of expression attribute name placeholders to actual attribute names
	 */
	Map<String, String> conditionNames() {
		Map<String,String> result = [:]
		map.each { k, v ->
			PathResult processed = processForKey(k)
			result.putAll(processed.nameMap)
		}
		return result
	}

	/**
	 * Gets the expression attribute values map for this key condition
	 *
	 * @return A map of expression attribute value placeholders to actual attribute values
	 */
	Map<String, AttributeValue> conditionValues() {
		map.collectEntries { k, v ->
			[ (":${safeValueName(k)}" as String): v ]
		}
	}

	/**
	 * Creates a key with a string attribute
	 *
	 * @param k The attribute name
	 * @param v The string value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String k, String v) {
		return new KeyMatch(k, v)
	}

	/**
	 * Creates a key with a numeric attribute
	 *
	 * @param k The attribute name
	 * @param v The numeric value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String k, Number v) {
		return new KeyMatch(k, v)
	}

	/**
	 * Creates a key with a binary attribute from byte array
	 *
	 * @param k The attribute name
	 * @param v The binary value as byte array
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String k, byte[] v) {
		return new KeyMatch(k, v)
	}

	/**
	 * Creates a key with a binary attribute from SdkBytes
	 *
	 * @param k The attribute name
	 * @param v The binary value as SdkBytes
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String k, SdkBytes v) {
		return new KeyMatch(k, v)
	}

	/**
	 * Creates a composite key with string partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, String pv, String sk, String sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with string partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, String pv, String sk, Number sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, String pv, String sk, byte[] sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, String pv, String sk, SdkBytes sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, Number pv, String sk, String sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, Number pv, String sk, Number sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, Number pv, String sk, byte[] sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, Number pv, String sk, SdkBytes sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, byte[] pv, String sk, String sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, SdkBytes pv, String sk, String sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, byte[] pv, String sk, Number sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, SdkBytes pv, String sk, Number sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, byte[] pv, String sk, byte[] sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 * @return A new KeyMatch instance
	 */
	static KeyMatch of(String pk, SdkBytes pv, String sk, SdkBytes sv) {
		return new KeyMatch(pk, pv, sk, sv)
	}

	/**
	 * Creates a key from a map of attribute names to AttributeValue objects
	 *
	 * @param key The key map (must contain 1 or 2 entries)
	 * @return A new KeyMatch instance
	 * @throws IllegalArgumentException if key size is invalid or contains unsupported types
	 */
	static KeyMatch of(Map<String, AttributeValue> key) {
		return new KeyMatch(key)
	}
}
