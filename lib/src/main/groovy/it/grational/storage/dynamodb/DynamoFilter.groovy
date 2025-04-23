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
	static DynamoFilter compare ( // {{{
		String name,
		String operator,
		Number value
	) {
		compare (
			name,
			operator,
			fromN(value.toString())
		)
	} // }}}

	/**
	 * Creates a comparison filter with the specified operator for string values
	 * @param operator One of: >, <, >=, <=, =, <>
	 */
	static DynamoFilter compare ( // {{{
		String name,
		String operator,
		String value
	) {
		compare (
			name,
			operator,
			fromS(value)
		)
	} // }}}

	static DynamoFilter compare ( // {{{
		String name,
		String operator,
		AttributeValue value
	) {
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
			[(vph): value]
		)
	} // }}}

	// scalar comparison methods {{{
	static DynamoFilter greater(String name, Number value) {
		return compare(name, ">", value)
	}
	static DynamoFilter greater(String name, String value) {
		return compare(name, ">", value)
	}

	static DynamoFilter greaterOrEqual(String name, Number value) {
		return compare(name, ">=", value)
	}
	static DynamoFilter greaterOrEqual(String name, String value) {
		return compare(name, ">=", value)
	}

	static DynamoFilter less(String name, Number value) {
		return compare(name, "<", value)
	}
	static DynamoFilter less(String name, String value) {
		return compare(name, "<", value)
	}

	static DynamoFilter lessOrEqual(String name, Number value) {
		return compare(name, "<=", value)
	}
	static DynamoFilter lessOrEqual(String name, String value) {
		return compare(name, "<=", value)
	}
	// }}}

	/**
	 * Creates a comparison filter between two attributes
	 * @param name1 First attribute name
	 * @param operator One of: >, <, >=, <=, =, <>
	 * @param name2 Second attribute name to compare with
	 */
	static DynamoFilter compareAttributes ( // {{{
		String name1,
		String operator,
		String name2
	) {
		String safe1 = safe(name1)
		String safe2 = safe(name2)
		String safeOperator = operator.trim()
		String nph1 = "#attr_${safe1}"
		String nph2 = "#attr_${safe2}"

		if (!['>', '<', '>=', '<=', '=', '<>'].contains(safeOperator))
			throw new IllegalArgumentException("Unsupported operator: ${operator}")

		String fe = "${nph1} ${safeOperator} ${nph2}"
		return new DynamoFilter(
			fe,
			[(nph1): name1, (nph2): name2],
			[:]  // No expression values needed since we're comparing attributes
		)
	} // }}}

	// attributes comparison methods {{{
	/**
	 * Creates an "attribute greater than other attribute" comparison filter
	 */
	static DynamoFilter attributeGreaterThan(String name1, String name2) {
		compareAttributes(name1, ">", name2)
	}

	/**
	 * Creates an "attribute greater than or equal to other attribute" comparison filter
	 */
	static DynamoFilter attributeGreaterOrEqual(String name1, String name2) {
		compareAttributes(name1, ">=", name2)
	}

	/**
	 * Creates an "attribute less than other attribute" comparison filter
	 */
	static DynamoFilter attributeLessThan(String name1, String name2) {
		compareAttributes(name1, "<", name2)
	}

	/**
	 * Creates an "attribute less than or equal to other attribute" comparison filter
	 */
	static DynamoFilter attributeLessOrEqual(String name1, String name2) {
		compareAttributes(name1, "<=", name2)
	}

	/**
	 * Creates an "attribute equals other attribute" comparison filter
	 */
	static DynamoFilter attributeEquals(String name1, String name2) {
		compareAttributes(name1, "=", name2)
	}

	/**
	 * Creates an "attribute not equals other attribute" comparison filter
	 */
	static DynamoFilter attributeNotEquals(String name1, String name2) {
		compareAttributes(name1, "<>", name2)
	}
	// }}}

	/**
	 * Creates a filter checking if an attribute's value is in a list of string values
	 * @param name The attribute name
	 * @param values The list of string values to check against
	 */
	static DynamoFilter in(String name, String... values) { // {{{
		if (values.length == 0)
			throw new IllegalArgumentException (
				'At least one value must be provided for IN filter'
			)

		String safe = safe(name)
		String nph = "#attr_${safe}"

		List<String> conditions = []
		Map<String, AttributeValue> expressionValues = [:]

		values.eachWithIndex { String value, int index ->
			String vph = ":val_${safe}_${index}"
			String condition = "${nph} = ${vph}"
			conditions.add(condition.toString())
			expressionValues[vph] = fromS(value)
		}

		String fe = '(' + conditions.join(' OR ') + ')'

		return new DynamoFilter (
			fe,
			[(nph): name],
			expressionValues
		)
	} // }}}

	/**
	 * Creates a filter checking if an attribute's value is in a list of numeric values
	 * @param name The attribute name
	 * @param values The list of numeric values to check against
	 */
	static DynamoFilter in(String name, Number... values) { // {{{
		if (values.length == 0)
			throw new IllegalArgumentException (
				'At least one value must be provided for IN filter'
			)

		String safe = safe(name)
		String nph = "#attr_${safe}"

		List<String> conditions = []
		Map<String, AttributeValue> expressionValues = [:]

		values.eachWithIndex { Number value, int index ->
			String vph = ":val_${safe}_${index}"
			String condition = "${nph} = ${vph}"
			conditions.add(condition.toString())
			expressionValues[vph] = fromN(value.toString())
		}

		String fe = "(" + conditions.join(' OR ') + ")"

		return new DynamoFilter (
			fe,
			[(nph): name],
			expressionValues
		)
	} // }}}

	/**
	 * Creates a filter checking if an attribute's value is between two string values (inclusive)
	 * @param name The attribute name
	 * @param start The lower bound value (inclusive)
	 * @param end The upper bound value (inclusive)
	 */
	static DynamoFilter between ( // {{{
		String name,
		String start,
		String end
	) {
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vphStart = ":val_${safe}_start"
		String vphEnd = ":val_${safe}_end"
		String fe = "${nph} BETWEEN ${vphStart} AND ${vphEnd}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[
				(vphStart): fromS(start),
				(vphEnd): fromS(end)
			]
		)
	} // }}}

	/**
	 * Creates a filter checking if an attribute's value is between two numeric values (inclusive)
	 * @param name The attribute name
	 * @param start The lower bound value (inclusive)
	 * @param end The upper bound value (inclusive)
	 */
	static DynamoFilter between ( // {{{
		String name,
		Number start,
		Number end
	) {
		String safe = safe(name)
		String nph = "#attr_${safe}"
		String vphStart = ":val_${safe}_start"
		String vphEnd = ":val_${safe}_end"
		String fe = "${nph} BETWEEN ${vphStart} AND ${vphEnd}"

		return new DynamoFilter (
			fe,
			[(nph): name],
			[
				(vphStart): fromN(start.toString()),
				(vphEnd): fromN(end.toString())
			]
		)
	} // }}}

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

	private DynamoFilter merge ( // {{{
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
	} // }}}

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
