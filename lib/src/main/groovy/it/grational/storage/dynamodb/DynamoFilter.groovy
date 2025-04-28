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
	String expression
	Map<String, String> expressionNames
	Map<String, AttributeValue> expressionValues
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
	static DynamoFilter matchAny ( // {{{
		String name,
		Number first,
		Number... rest
	) { 
		List<Number> combined = [ first ] + rest.toList()
		return commonIn (
			name,
			combined.collect { Number n -> fromN(n.toString()) }
		)
	} // }}}

	static DynamoFilter matchAny ( // {{{
		String name,
		String first,
		String... rest
	) {
		List<String> combined = [ first ] + rest.toList()
		return commonIn (
			name,
			combined.collect { String s -> fromS(s) }
		)
	} // }}}

	private static DynamoFilter commonIn ( // {{{
		String name,
		List<AttributeValue> values
	) {
		String safe = safe(name)
		String nph  = "#attr_${safe}"

		List<String> placeholders           = []
		Map<String, AttributeValue> eValues = [:]

		values.eachWithIndex { AttributeValue av, int i ->
			String vph = ":val_${safe}_${i}"
			placeholders << vph
			eValues[vph] = av
		}

		String fe = "${nph} IN (${placeholders.join(', ')})"

		return new DynamoFilter (
			fe,
			[(nph): name],
			eValues
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
		return commonBetween (
			name,
			fromS(start),
			fromS(end)
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
		return commonBetween (
			name,
			fromN(start.toString()),
			fromN(end.toString())
		)
	} // }}}

	static DynamoFilter commonBetween ( // {{{
		String name,
		AttributeValue start,
		AttributeValue end
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
				(vphStart): start,
				(vphEnd): end
			]
		)
	} // }}}

	/**
	 * Negates this filter condition
	 */
	DynamoFilter not() { // {{{
		String expr = this.expression
		String negated = needsGrouping(expr)
			? "NOT (${expr})"
			: "NOT ${expr}"
		return new DynamoFilter (
			negated,
			this.expressionNames,
			this.expressionValues
		)
	} // }}}

	DynamoFilter and(DynamoFilter first, DynamoFilter... rest) { // {{{
		combineMany('AND', [this, first] + rest.toList())
	} // }}}

	DynamoFilter or(DynamoFilter first, DynamoFilter... rest) { // {{{
		combineMany('OR', [this, first] + rest.toList())
	} // }}}

	static DynamoFilter every ( // {{{
		DynamoFilter a,
		DynamoFilter b,
		DynamoFilter... others
	) {
		combineMany('AND', [ a, b ] + others.toList())
	} // }}}

	static DynamoFilter any ( // {{{
		DynamoFilter a,
		DynamoFilter b,
		DynamoFilter... others
	) { 
		combineMany('OR', [ a, b ] + others.toList())
	} // }}}

	private static DynamoFilter combineMany (
		String operator,
		List<DynamoFilter> filters
	) { // {{{
		Map<String, String> names = [:]
		Map<String, AttributeValue> values = [:]
		filters.each { DynamoFilter df ->
			names += df.expressionNames
			values = mergeValues(values, df)
		}

		String expr = filters.collect { DynamoFilter df ->
			boolean needsParens =
				df.expression.contains(' AND ') ||
				df.expression.contains(' OR ')  ||
				df.expression.startsWith('NOT ')
			needsParens ? "(${df.expression})" : df.expression
		}.join(" ${operator} ")

		return new DynamoFilter(expr, names, values)
	} // }}}

	private static Map<String, AttributeValue> mergeValues (
		Map<String, AttributeValue> combined,
		DynamoFilter filter
	) { // {{{
		Map<String, AttributeValue> result = combined
		int counter; String key
		filter.expressionValues.each { String k, AttributeValue v ->
			counter = 1
			key = k

			while (result.containsKey(key))
				key = "${k}_${counter++}"

			if ( k != key )
				filter.expression = filter.expression.replaceAll(k, key)

			result[key] = v
		}
		return result
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

	private static boolean needsGrouping(String expr) {
		expr.contains(' AND ') || expr.contains(' OR ')
	}

}
// vim: fdm=marker
