package it.grational.storage.dynamodb

// imports {{{
import groovy.transform.ToString
import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
// }}}

/**
 * Fluent builder for DynamoDB filter expressions.
 * Use it with static imports to create filter expressions in a readable way.
 * import static it.grational.storage.dynamodb.DynamoFilter.*
 */
@ToString (
	includePackage=false,
	includeFields=true,
	includeNames=true
)
@CompileStatic
class DynamoFilter {
	// fields {{{
	private final String expression
	private final Map<String, String> expressionNames
	private final Map<String, AttributeValue> expressionValues
	// }}}

	DynamoFilter (
		String fe,
		Map<String, String> ean,
		Map<String, AttributeValue> eav
	) { // {{{
		this.expression = fe
		this.expressionNames = ean
		this.expressionValues = eav
	} // }}}

	// Static factory methods - for use with static imports
	
	/**
	 * Creates a filter checking if an attribute is blank (null or doesn't exist)
	 */
	static DynamoFilter isBlank(String name) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "attribute_not_exists(${nph}) OR ${nph} = ${vph}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromNul(true)]
		)
	} // }}}

	/**
	 * Creates a filter checking if an attribute is not blank
	 */
	static DynamoFilter isNotBlank(String name) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "attribute_exists(${nph}) AND NOT ${nph} = ${vph}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromNul(true)]
		)
	} // }}}

	/**
	 * Creates an match filter for string values
	 */
	static DynamoFilter match(String name, String value) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "${nph} = ${vph}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromS(value)]
		)
	} // }}}

	/**
	 * Creates an match filter for numeric values
	 */
	static DynamoFilter match(String name, Number value) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "${nph} = ${vph}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromN(value.toString())]
		)
	} // }}}

	/**
	 * Creates an match filter for boolean values
	 */
	static DynamoFilter match(String name, boolean value) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "${nph} = ${vph}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromBool(value)]
		)
	} // }}}

	/**
	 * Creates a filter checking if an attribute contains the specified string
	 */
	static DynamoFilter contains(String name, String value) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "contains(${nph}, ${vph})"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromS(value)]
		)
	} // }}}

	/**
	 * Creates a filter checking if an attribute begins with the specified string
	 */
	static DynamoFilter beginsWith(String name, String value) { // {{{
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"
		String fe = "begins_with(${nph}, ${vph})"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromS(value)]
		)
	} // }}}

	/**
	 * Creates a comparison filter with the specified operator
	 * @param operator One of: >, <, >=, <=, =, <>
	 */
	static DynamoFilter compare (
		String name,
		String operator,
		Number value
	) { // {{{
		String safe = safe(name)
		String safeOperator = operator.trim()
		String nph = "#attr_${safe}"
		String vph = ":val_${safe}"

		if (!['>', '<', '>=', '<=', '=', '<>'].contains(safeOperator))
			throw new IllegalArgumentException("Unsupported operator: ${operator}")

		String fe = "${nph} ${safeOperator} ${vph}"
		return new DynamoFilter (
			fe,
			[(nph): name],
			[(vph): fromN(value.toString())]
		)
	} // }}}

	// Convenience methods for common comparisons
	
	/**
	 * Creates a "greater than" comparison filter
	 */
	static DynamoFilter greater(String name, Number value) {
		return compare(name, ">", value)
	}
	
	/**
	 * Creates a "greater than or equal" comparison filter
	 */
	static DynamoFilter greaterOrEqual(String name, Number value) {
		return compare(name, ">=", value)
	}
	
	/**
	 * Creates a "less than" comparison filter
	 */
	static DynamoFilter less(String name, Number value) {
		return compare(name, "<", value)
	}
	
	/**
	 * Creates a "less than or equal" comparison filter
	 */
	static DynamoFilter lessOrEqual(String name, Number value) {
		return compare(name, "<=", value)
	}

	/**
	 * Combines this filter with another using AND
	 */
	DynamoFilter and(DynamoFilter other) { // {{{
		return merge(this, 'AND', other)
	} // }}}

	/**
	 * Combines this filter with another using OR
	 */
	DynamoFilter or(DynamoFilter other) { // {{{
		return merge(this, 'OR', other)
	} // }}}

	private DynamoFilter merge (
		DynamoFilter a,
		String operator,
		DynamoFilter b
	) {
		Set<String> commonValues = a.expressionValues.keySet()
		.intersect (
			b.expressionValues.keySet()
		)
		Map<String,String> commonNames = a.expressionNames + b.expressionNames
		if ( commonValues.isEmpty() )
			return new DynamoFilter (
			"(${a.expression}) ${operator} (${b.expression})",
				commonNames,
				a.expressionValues + b.expressionValues
			)

		Map<String, String> keyMappings = [:]
		Map<String, AttributeValue> newValues = [:]

		newValues.putAll(a.expressionValues)

		String bExpression = b.expression
		int counter = 1
		b.expressionValues.each { key, value ->
			if (commonValues.contains(key)) {
				String newKey = "${key}_${counter++}"
				keyMappings[key] = newKey

				bExpression = bExpression.replace(key, newKey)

				newValues[newKey] = value
			} else {
				newValues[key] = value
			}
		}

		return new DynamoFilter (
			"(${a.expression}) ${operator} (${bExpression})",
			commonNames,
			newValues
		)
	}

	/**
	 * Negates this filter condition
	 */
	DynamoFilter not() { // {{{
		String negated = "NOT (${this.expression})"
		return new DynamoFilter (
			negated,
			this.expressionNames,
			this.expressionValues
		)
	} // }}}

	String getExpression() { // {{{
		return expression
	} // }}}

	Map<String, String> getExpressionNames() { // {{{
		return expressionNames
	} // }}}

	Map<String, AttributeValue> getExpressionValues() { // {{{
		return expressionValues
	} // }}}

	static private String safe(String name) { // {{{
		name.replaceAll(/[^a-zA-Z0-9_]/,'')
	} // }}}
}
// vim: fdm=marker
