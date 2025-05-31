package it.grational.storage.dynamodb

import groovy.transform.ToString
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import static it.grational.storage.dynamodb.NestedPathProcessor.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import software.amazon.awssdk.core.SdkBytes
import static software.amazon.awssdk.core.SdkBytes.*
import static it.grational.storage.dynamodb.DynamoFilter.*

/**
 * Represents a key condition for DynamoDB operations.
 *
 * KeyFilter is used to represent key conditions when querying or retrieving items from DynamoDB.
 * It supports both single-key (partition key only) and composite key (partition + sort key) conditions
 * with various data types (string, number, binary).
 *
 * Example usage:
 * <pre>
 * // Simple partition key only
 * KeyFilter simpleKey = new KeyFilter("userId", "user123")
 * // or using static factory method
 * KeyFilter simpleKey = KeyFilter.of("userId", "user123")
 *
 * // Composite key with string + number
 * KeyFilter compositeKey = new KeyFilter("userId", "user123", "timestamp", 1621234567)
 * // or using static factory method
 * KeyFilter compositeKey = KeyFilter.of("userId", "user123", "timestamp", 1621234567)
 *
 * // Access components
 * KeyFilter partOnly = compositeKey.partition() // Only userId=user123
 * Optional<KeyFilter> sortOnly = compositeKey.sort() // Only timestamp=1621234567
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
class KeyFilter {
	private final Map<String, AttributeValue> map = [:]
	private Optional<DynamoFilter> sortFilter = Optional.empty()

	/**
	 * Constructor that accepts a map of attribute names to AttributeValue objects
	 *
	 * @param key The key map (must contain 1 or 2 entries)
	 * @throws IllegalArgumentException if key size is invalid or contains unsupported types
	 */
	KeyFilter(Map<String, AttributeValue> key) {
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
	 * Private constructor for KeyFilter with sort key filter
	 *
	 * @param key The key map (partition key only)
	 * @param sortFilter The DynamoFilter for sort key range conditions
	 */
	private KeyFilter (
		Map<String, AttributeValue> key,
		DynamoFilter sortFilter
	) {
		if ( key.size() != 1 )
			throw new IllegalArgumentException (
				"Key with sort filter must contain only partition key, got size: ${key.size()}"
			)
		if ( key.any { k, v -> v.type() !in [ Type.S, Type.N, Type.B ] } )
			throw new IllegalArgumentException (
				"Unsupported key types: ${key}"
			)
		map = key
		this.sortFilter = Optional.of(sortFilter)
	}

	/**
	 * Creates a key with a string attribute
	 *
	 * @param k The attribute name
	 * @param v The string value
	 */
	KeyFilter(String k, String v) {
		map << [ (k): fromS(v) ]
	}

	/**
	 * Creates a key with a numeric attribute
	 *
	 * @param k The attribute name
	 * @param v The numeric value
	 */
	KeyFilter(String k, Number v) {
		map << [ (k): fromN(v.toString()) ]
	}

	/**
	 * Creates a key with a binary attribute from byte array
	 *
	 * @param k The attribute name
	 * @param v The binary value as byte array
	 */
	KeyFilter(String k, byte[] v) {
		this(k, fromByteArray(v))
	}

	/**
	 * Creates a key with a binary attribute from SdkBytes
	 *
	 * @param k The attribute name
	 * @param v The binary value as SdkBytes
	 */
	KeyFilter(String k, SdkBytes v) {
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
	KeyFilter(String pk, String pv, String sk, String sv) {
		map << [ (pk): fromS(pv) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with string partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyFilter(String pk, String pv, String sk, Number sv) {
		map << [ (pk): fromS(pv) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 */
	KeyFilter(String pk, String pv, String sk, byte[] sv) {
		map << [ (pk): fromS(pv) ]
		sortFilter = Optional.of (
			compare(sk, '=', fromB(fromByteArray(sv)))
		)
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 */
	KeyFilter(String pk, String pv, String sk, SdkBytes sv) {
		map << [ (pk): fromS(pv) ]
		sortFilter = Optional.of (
			compare(sk, '=', fromB(sv))
		)
	}

	/**
	 * Creates a composite key with numeric partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyFilter(String pk, Number pv, String sk, String sv) {
		map << [ (pk): fromN(pv.toString()) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with numeric partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyFilter(String pk, Number pv, String sk, Number sv) {
		map << [ (pk): fromN(pv.toString()) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 */
	KeyFilter(String pk, Number pv, String sk, byte[] sv) {
		map << [ (pk): fromN(pv.toString()) ]
		sortFilter = Optional.of (
			compare(sk, '=', fromB(fromByteArray(sv)))
		)
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 */
	KeyFilter(String pk, Number pv, String sk, SdkBytes sv) {
		map << [ (pk): fromN(pv.toString()) ]
		sortFilter = Optional.of (
			compare(sk, '=', fromB(sv))
		)
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyFilter(String pk, byte[] pv, String sk, String sv) {
		map << [ (pk): fromB(fromByteArray(pv)) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 */
	KeyFilter(String pk, SdkBytes pv, String sk, String sv) {
		map << [ (pk): fromB(pv) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyFilter(String pk, byte[] pv, String sk, Number sv) {
		map << [ (pk): fromB(fromByteArray(pv)) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 */
	KeyFilter(String pk, SdkBytes pv, String sk, Number sv) {
		map << [ (pk): fromB(pv) ]
		sortFilter = Optional.of(match(sk, sv))
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 */
	KeyFilter(String pk, byte[] pv, String sk, byte[] sv) {
		map << [ (pk): fromB(fromByteArray(pv)) ]
		sortFilter = Optional.of (
			compare(sk, '=', fromB(fromByteArray(sv)))
		)
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 */
	KeyFilter(String pk, SdkBytes pv, String sk, SdkBytes sv) {
		map << [ (pk): fromB(pv) ]
		sortFilter = Optional.of (
			compare(sk, '=', fromB(sv))
		)
	}

	/**
	 * Converts the key to a DynamoDB attribute value map
	 *
	 * @return A map of attribute names to AttributeValue objects
	 */
	Map<String,AttributeValue> toMap() {
		if (sortFilter.isPresent()) {
			// For backward compatibility, try to reconstruct the traditional map format
			// for simple equality conditions
			def filter = sortFilter.get()
			def sortKeyInfo = matchFromFilter(filter)
			if (sortKeyInfo) {
				Map<String, AttributeValue> result = new LinkedHashMap<>(map)
				String sortKeyName = (String) sortKeyInfo.get('name')
				AttributeValue sortKeyValue = (AttributeValue) sortKeyInfo.get('value')
				result.put(sortKeyName, sortKeyValue)
				return result
			}
		}
		return map
	}
	
	/**
	 * Helper method to extract sort key name and value from a simple match DynamoFilter
	 * Returns null if the filter is not a simple equality match
	 */
	private Map<String, Object> matchFromFilter(DynamoFilter filter) {
		// Check if this is a simple equality expression
		// (e.g., "#attr_name = :val_name" or "#attr_name.#attr_nested = :val_name")
		String expr = filter.expression
		if (expr.matches(/^#[\w_]+(\\.#[\w_]+)* = :[\w_]+$/)) {
			// Simple equality pattern found
			def names = filter.expressionNames
			def values = filter.expressionValues
			
			// Extract the value (should be only one for simple match)
			if (values.size() == 1) {
				String valueRef = values.keySet().first()
				AttributeValue actualValue = values[valueRef]
				
				// Reconstruct the full attribute name from nested parts
				String fullName = names.values().join('.')
				
				Map<String, Object> result = [:]
				result.put('name', fullName)
				result.put('value', actualValue)
				return result
			}
		}
		return null
	}

	/**
	 * Checks if this is a composite key (has both partition and sort keys)
	 *
	 * @return true if the key has both partition and sort components, false if it's a partition-only key
	 */
	boolean composite() {
		map.size() > 1 || sortFilter.isPresent()
	}

	/**
	 * Gets a KeyFilter representing only the partition key component
	 *
	 * @return A new KeyFilter containing only the partition key, or this object if it's already a partition-only key
	 */
	KeyFilter partition() {
		return ( composite() )
			? new KeyFilter(map.take(1))
			: this
	}

	/**
	 * Gets an Optional KeyFilter representing only the sort key component
	 *
	 * @return An Optional containing a new KeyFilter with just the sort key, or empty if this is a partition-only key
	 */
	Optional<KeyFilter> sort() {
		if (sortFilter.isPresent()) {
			// Try to extract sort key from simple match conditions
			def filter = sortFilter.get()
			def sortKeyInfo = matchFromFilter(filter)
			if (sortKeyInfo) {
				String sortKeyName = (String) sortKeyInfo.get('name')
				AttributeValue sortKeyValue = (AttributeValue) sortKeyInfo.get('value')
				Map<String, AttributeValue> sortKeyMap = [(sortKeyName): sortKeyValue]
				return Optional.of(new KeyFilter(sortKeyMap))
			}
			return Optional.empty() // Cannot extract sort key from complex range conditions
		}
		return Optional.ofNullable (
			map.size() > 1
				? new KeyFilter(map.drop(1))
				: null
		)
	}

	/**
	 * Builds a key condition expression for DynamoDB operations
	 *
	 * @return A string representing the key condition expression
	 */
	String condition() {
		String partitionCondition = map.collect { k, v ->
			PathResult processed = processForKey(k)
			"${processed.nameRef} = :${safeValueName(k)}"
		}.join(' AND ')
		
		if (sortFilter.isPresent()) {
			return "${partitionCondition} AND ${sortFilter.get().expression}"
		}
		
		return partitionCondition
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
		
		if (sortFilter.isPresent()) {
			result.putAll(sortFilter.get().expressionNames)
		}
		
		return result
	}

	/**
	 * Gets the expression attribute values map for this key condition
	 *
	 * @return A map of expression attribute value placeholders to actual attribute values
	 */
	Map<String, AttributeValue> conditionValues() {
		Map<String, AttributeValue> result = map.collectEntries { k, v ->
			[ (":${safeValueName(k)}" as String): v ]
		}
		
		if (sortFilter.isPresent()) {
			result.putAll(sortFilter.get().expressionValues)
		}
		
		return result
	}

	/**
	 * Creates a key with a string attribute
	 *
	 * @param k The attribute name
	 * @param v The string value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String k, String v) {
		return new KeyFilter(k, v)
	}

	/**
	 * Creates a key with a numeric attribute
	 *
	 * @param k The attribute name
	 * @param v The numeric value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String k, Number v) {
		return new KeyFilter(k, v)
	}

	/**
	 * Creates a key with a binary attribute from byte array
	 *
	 * @param k The attribute name
	 * @param v The binary value as byte array
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String k, byte[] v) {
		return new KeyFilter(k, v)
	}

	/**
	 * Creates a key with a binary attribute from SdkBytes
	 *
	 * @param k The attribute name
	 * @param v The binary value as SdkBytes
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String k, SdkBytes v) {
		return new KeyFilter(k, v)
	}

	/**
	 * Creates a composite key with string partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, String pv, String sk, String sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with string partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, String pv, String sk, Number sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, String pv, String sk, byte[] sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with string partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, String pv, String sk, SdkBytes sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, Number pv, String sk, String sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, Number pv, String sk, Number sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, Number pv, String sk, byte[] sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with numeric partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, Number pv, String sk, SdkBytes sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, byte[] pv, String sk, String sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and string sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key string value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, SdkBytes pv, String sk, String sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, byte[] pv, String sk, Number sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and numeric sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key numeric value
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, SdkBytes pv, String sk, Number sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sk The sort key name
	 * @param sv The sort key binary value as byte array
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, byte[] pv, String sk, byte[] sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a composite key with binary partition and binary sort values
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sk The sort key name
	 * @param sv The sort key binary value as SdkBytes
	 * @return A new KeyFilter instance
	 */
	static KeyFilter of(String pk, SdkBytes pv, String sk, SdkBytes sv) {
		return new KeyFilter(pk, pv, sk, sv)
	}

	/**
	 * Creates a key from a map of attribute names to AttributeValue objects
	 *
	 * @param key The key map (must contain 1 or 2 entries)
	 * @return A new KeyFilter instance
	 * @throws IllegalArgumentException if key size is invalid or contains unsupported types
	 */
	static KeyFilter of(Map<String, AttributeValue> key) {
		return new KeyFilter(key)
	}
	
	/**
	 * Creates a KeyFilter with sort key conditions using DynamoFilter
	 *
	 * @param pk The partition key name
	 * @param pv The partition key string value
	 * @param sortFilter The DynamoFilter for sort key conditions
	 * @return A new KeyFilter instance with sort key filter support
	 */
	static KeyFilter of(String pk, String pv, DynamoFilter sortFilter) {
		return new KeyFilter([(pk): fromS(pv)], sortFilter)
	}
	
	/**
	 * Creates a KeyFilter with sort key conditions using DynamoFilter
	 *
	 * @param pk The partition key name
	 * @param pv The partition key numeric value
	 * @param sortFilter The DynamoFilter for sort key conditions
	 * @return A new KeyFilter instance with sort key filter support
	 */
	static KeyFilter of(String pk, Number pv, DynamoFilter sortFilter) {
		return new KeyFilter([(pk): fromN(pv.toString())], sortFilter)
	}
	
	/**
	 * Creates a KeyFilter with sort key conditions using DynamoFilter
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as byte array
	 * @param sortFilter The DynamoFilter for sort key conditions
	 * @return A new KeyFilter instance with sort key filter support
	 */
	static KeyFilter of(String pk, byte[] pv, DynamoFilter sortFilter) {
		return new KeyFilter([(pk): fromB(fromByteArray(pv))], sortFilter)
	}
	
	/**
	 * Creates a KeyFilter with sort key conditions using DynamoFilter
	 *
	 * @param pk The partition key name
	 * @param pv The partition key binary value as SdkBytes
	 * @param sortFilter The DynamoFilter for sort key conditions
	 * @return A new KeyFilter instance with sort key filter support
	 */
	static KeyFilter of(String pk, SdkBytes pv, DynamoFilter sortFilter) {
		return new KeyFilter([(pk): fromB(pv)], sortFilter)
	}
}
