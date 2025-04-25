package it.grational.storage.dynamodb

// imports {{{
import spock.lang.*
import static it.grational.storage.dynamodb.DynamoFilter.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

class DynamoFilterUSpec extends Specification {

	def "Should be able to create 'match' filters for string values"() { // {{{
		when:
			def filter = match('status', 'ACTIVE')

		then:
			filter.expression == '#attr_status = :val_status'
			filter.expressionNames == ['#attr_status': 'status']
			filter.expressionValues[':val_status'].s() == 'ACTIVE'
	} // }}}

	def "Should be able to create 'match' filters for numeric value"() { // {{{
		when:
			def filter = match('amount', 100)

		then:
			filter.expression == '#attr_amount = :val_amount'
			filter.expressionNames == ['#attr_amount': 'amount']
			filter.expressionValues[':val_amount'].n() == '100'
	} // }}}

	def "Should be able to create 'contains' filter"() { // {{{
		when:
			def filter = contains('keywords', 'social')

		then:
			filter.expression == 'contains(#attr_keywords, :val_keywords)'
			filter.expressionNames == ['#attr_keywords': 'keywords']
			filter.expressionValues[':val_keywords'].s() == 'social'
	} // }}}

	def "Should be able to create 'begins_with' filters"() { // {{{
		when:
			def filter = beginsWith('id', 'prod_')

		then:
			filter.expression == 'begins_with(#attr_id, :val_id)'
			filter.expressionNames == ['#attr_id': 'id']
			filter.expressionValues[':val_id'].s() == 'prod_'
	} // }}}

	def "Should be able to create 'operators' filters"() { // {{{
		when:
			def filter = compare('price', op, 50)

		then:
			filter.expression == "#attr_price ${op} :val_price"
			filter.expressionNames == ['#attr_price': 'price']
			filter.expressionValues[':val_price'].n() == '50'

		where:
			op << [
				'>',
				'>=',
				'=',
				'<',
				'<=',
				'<>'
			]
	} // }}}

	def "Should reject invalid operators"() { // {{{
		when:
			compare('price', '!!', 50)

		then:
			def e = thrown(IllegalArgumentException)
			e.message == 'Unsupported operator: !!'
	} // }}}

	def "Should be able to combine filters with the AND operator"() { // {{{
		given:
			def filter1 = match('status', 'ACTIVE')
			def filter2 = compare('price', '>', 100)

		when:
			def combined = filter1.and(filter2)

		then:
			combined.expression == '#attr_status = :val_status AND #attr_price > :val_price'
			combined.expressionNames.size() == 2
			combined.expressionValues.size() == 2
	} // }}}

	def "Should be able to combine filters with the OR operator"() { // {{{
		given:
			def filter1 = match('type', 'PREMIUM')
			def filter2 = compare('credits', '>=', 1000)

		when:
			def combined = filter1.or(filter2)

		then:
			combined.expression == '#attr_type = :val_type OR #attr_credits >= :val_credits'
			combined.expressionNames.size() == 2
			combined.expressionValues.size() == 2
	} // }}}

	def "Should be able to negate a filter"() { // {{{
		given:
			def filter = match('status', 'DELETED')

		when:
			def negated = filter.not()

		then:
			negated.expression == 'NOT #attr_status = :val_status'
			negated.expressionNames.size() == 1
			negated.expressionValues.size() == 1
	} // }}}

	def "Should be able to combine multiple filters"() { // {{{
		given:
			def statusFilter = match('status', 'ACTIVE')
			def typeFilter = match('type', 'PREMIUM')
			def priceFilter = compare('price', '>=', 50)
			def keywordFilter = contains('description', 'special')

		when:
			def complex = statusFilter
				.and(typeFilter)
				.and(priceFilter.or(keywordFilter).not())

		then:
			complex.expression == '(#attr_status = :val_status AND #attr_type = :val_type) AND (NOT (#attr_price >= :val_price OR contains(#attr_description, :val_description)))'
			complex.expressionNames.size() == 4
			complex.expressionValues.size() == 4
	} // }}}

	def "Should be able to create match filters for boolean values"() { // {{{
		when:
			def filter = match('active', true)

		then:
			filter.expression == '#attr_active = :val_active'
			filter.expressionNames == ['#attr_active': 'active']
			filter.expressionValues[':val_active'].bool() == true

		when:
			def falseFilter = match('canceled', false)

		then:
			falseFilter.expressionValues[':val_canceled'].bool() == false
	} // }}}

	def "Should create a filter for checking if an attribute is blank"() { // {{{
		when:
			def filter = isBlank('status')

		then:
			filter.expression == 'attribute_not_exists(#attr_status) OR #attr_status = :val_status'
			filter.expressionNames == ['#attr_status': 'status']
			filter.expressionValues.containsKey(':val_status')
			filter.expressionValues[':val_status'].nul()
	} // }}}

	def "Should create a filter for checking if an attribute is not blank"() { // {{{
		when:
			def filter = isNotBlank('status')

		then:
			filter.expression == 'attribute_exists(#attr_status) AND NOT #attr_status = :val_status'
			filter.expressionNames == ['#attr_status': 'status']
			filter.expressionValues.containsKey(':val_status')
			filter.expressionValues[':val_status'].nul()
	} // }}}

	def "Should be able to combine isBlank with other filters"() { // {{{
		given:
			def blankFilter = isBlank('status')
			def matchFilter = match('category', 'active')

		when:
			def combined = blankFilter.and(matchFilter)

		then:
			combined.expression.contains('attribute_not_exists')
			combined.expression.contains('#attr_category = :val_category')
			combined.expressionNames.size() == 2
			combined.expressionValues.size() == 2
	} // }}}

	def "Should be able to negate blank check filters"() { // {{{
		when:
			def filter = isBlank('status').not()

		then:
			filter.expression == 'NOT (attribute_not_exists(#attr_status) OR #attr_status = :val_status)'
	} // }}}

	def "Should maintain distinct values when filtering on the same attribute with different values"() { // {{{
		given:
			def filter1 = match('v', 1)
			def filter2 = match('v', 2)

		when:
			def combined = filter1.or(filter2)

		then:
			combined.expression == '#attr_v = :val_v OR #attr_v = :val_v_1'
		and:
			combined.expressionValues.size() == 2
		and:
			def values = combined.expressionValues.values().collect {
				it.n().toInteger()
			}
		and:
			values.containsAll([1, 2])
	} // }}}

	def "Should maintain distinct values when filtering with AND on same attributes"() { // {{{
		given:
			def filter1 = compare('price', '>=', 10)
			def filter2 = compare('price', '<=', 20)

		when:
			def priceRangeFilter = filter1.and(filter2)

		then:
			priceRangeFilter.expressionValues.size() == 2
		and:
			priceRangeFilter.expression.contains('>=')
			priceRangeFilter.expression.contains('<=')
		and:
			def values = priceRangeFilter.expressionValues.values().collect {
				it.n().toInteger()
			}
		and:
			values.containsAll([10, 20])
	} // }}}

	def "Should work with complex filter combinations using the same attributes"() { // {{{
		given:
			def statusActive = match('status', 'ACTIVE')
			def statusPending = match('status', 'PENDING')
			def priceHigh = compare('price', '>', 100)
			def priceLow = compare('price', '<', 20)

		when:
			def complexFilter = (statusActive.or(statusPending))
				.and(priceHigh.or(priceLow))

		then:
			complexFilter.expressionValues.size() == 4
		and:
			def statusValues = complexFilter.expressionValues.values()
				.findResults { it.s() }
		and:
			statusValues.containsAll(['ACTIVE', 'PENDING'])
		and:
			def priceValues = complexFilter.expressionValues.values()
				.findResults { it.n()?.toInteger() }
		and:
			priceValues.containsAll([100, 20])
	} // }}}

	def "Should create price range filters concisely"() { // {{{
		when:
			def filter = greaterOrEqual('price', 10)
				.and(lessOrEqual('price', 100))

		then:
			filter.expression.contains('>= :val_price')
			filter.expression.contains('<= :val_price')
			filter.expressionNames.size() == 1
			filter.expressionValues.size() == 2
	} // }}}

	def "Should handle compound filter expressions"() { // {{{
		when:
			def activeProducts = match('type', 'product')
				.and(match('status', 'active'))

			def featuredProducts = match('type', 'product')
				.and(match('featured', true))

			def filter = activeProducts.or(featuredProducts)

		then:
			filter.expression.contains('type')
			filter.expression.contains('status')
			filter.expression.contains('featured')
			filter.expression.contains('OR')
			filter.expression.contains('AND')
			filter.expressionNames.size() == 3
	} // }}}

	def "Should handle complex date range filters"() { // {{{
		when:
			def dateFilter = greaterOrEqual('created_at', 20250101)
				.and(less('created_at', 20250201))
				.and(isBlank('deleted_at'))

		then:
			dateFilter.expression.contains('>= :val_created_at')
			dateFilter.expression.contains('< :val_created_at')
			dateFilter.expression.contains('attribute_not_exists(#attr_deleted_at)')
			dateFilter.expressionNames.size() == 2
			dateFilter.expressionValues.size() == 3
	} // }}}

	def "Should support multiple comparison filters on different fields"() { // {{{
		when:
			def filter = greater('priority', 5)
				.and(less('price', 100))
				.and(match('category', 'electronics'))

		then:
			filter.expression.contains('> :val_priority')
			filter.expression.contains('< :val_price')
			filter.expression.contains('= :val_category')
			filter.expressionNames.size() == 3
			filter.expressionValues.size() == 3
	} // }}}

	def "Should support string comparisons"() { // {{{
		when:
			def filter = compare('name', op, 'John')

		then:
			filter.expression == "#attr_name ${op} :val_name"
			filter.expressionNames == ['#attr_name': 'name']
			filter.expressionValues[':val_name'].s() == 'John'

		where:
			op << [
				'>',
				'>=',
				'=',
				'<',
				'<=',
				'<>'
			]
	} // }}}

	def "Should support convenience methods for string comparisons"() { // {{{
		expect:
			greater('name', 'M').expression == '#attr_name > :val_name'
			greaterOrEqual('name', 'M').expression == '#attr_name >= :val_name'
			less('name', 'M').expression == '#attr_name < :val_name'
			lessOrEqual('name', 'M').expression == '#attr_name <= :val_name'

		and:
			greater('name', 'M').expressionValues[':val_name'].s() == 'M'
			greaterOrEqual('name', 'M').expressionValues[':val_name'].s() == 'M'
			less('name', 'M').expressionValues[':val_name'].s() == 'M'
			lessOrEqual('name', 'M').expressionValues[':val_name'].s() == 'M'
	} // }}}

	def "Should work with string comparisons in complex expressions"() { // {{{
		when:
			def filter = greater('lastname', 'N')
				.and(lessOrEqual('lastname', 'Z'))
				.and(greater('age', 21))

		then:
			filter.expression.contains('#attr_lastname > :val_lastname')
			filter.expression.contains('#attr_lastname <= :val_lastname_')
			filter.expression.contains('#attr_age > :val_age')
			filter.expressionNames.size() == 2
			filter.expressionValues.size() == 3
			filter.expressionValues.containsKey(':val_lastname')
			filter.expressionValues.containsKey(':val_lastname_1')
			filter.expressionValues.containsKey(':val_age')
			filter.expressionValues[':val_lastname'].s() == 'N'
			filter.expressionValues[':val_lastname_1'].s() == 'Z'
			filter.expressionValues[':val_age'].n() == '21'
	} // }}}

	def "Should create OR conditions for value ranges"() { // {{{
		when:
			def filter = less('price', 10).or (
				greater('price', 100)
			)

		then:
			filter.expression == '#attr_price < :val_price OR #attr_price > :val_price_1'
			filter.expressionNames.size() == 1
			filter.expressionValues.size() == 2
			filter.expressionValues.containsKey(':val_price')
			filter.expressionValues.containsKey(':val_price_1')
	} // }}}

	def "Should correctly combine filters with NOT operations"() { // {{{
		when:
			def filter = match('status', 'ACTIVE').and (
				lessOrEqual('price', 1000).not()
			)

		then:
			filter.expression.contains('= :val_status')
			filter.expression.contains('NOT #attr_price <= :val_price')
			filter.expressionNames.size() == 2
			filter.expressionValues.size() == 2
	} // }}}

	def "Should be able to create 'compareAttributes' filters"() { // {{{
		when:
			def filter = compareAttributes('firstField', op, 'secondField')

		then:
			filter.expression == "#attr_firstField ${op} #attr_secondField"
			filter.expressionNames == [
				'#attr_firstField': 'firstField',
				'#attr_secondField': 'secondField'
			]
			filter.expressionValues.isEmpty()

		where:
			op << [
				'>',
				'>=',
				'=',
				'<',
				'<=',
				'<>'
			]
	} // }}}

	def "Should reject invalid operators in compareAttributes"() { // {{{
		when:
			compareAttributes('firstField', '!!', 'secondField')

		then:
			def e = thrown(IllegalArgumentException)
			e.message == 'Unsupported operator: !!'
	} // }}}

	def "Should provide convenience methods for attribute comparisons"() { // {{{
		expect:
			attributeGreaterThan('a', 'b').expression == '#attr_a > #attr_b'
			attributeGreaterOrEqual('a', 'b').expression == '#attr_a >= #attr_b'
			attributeLessThan('a', 'b').expression == '#attr_a < #attr_b'
			attributeLessOrEqual('a', 'b').expression == '#attr_a <= #attr_b'
			attributeEquals('a', 'b').expression == '#attr_a = #attr_b'
			attributeNotEquals('a', 'b').expression == '#attr_a <> #attr_b'
	} // }}}

	def "Should be able to combine attribute comparison with other filters"() { // {{{
		given:
			def statusFilter = match('status', 'ACTIVE')
			def amountComparisonFilter = attributeGreaterThan('amount', 'threshold')

		when:
			def combined = statusFilter.and(amountComparisonFilter)

		then:
			combined.expression == '#attr_status = :val_status AND #attr_amount > #attr_threshold'
			combined.expressionNames.size() == 3
			combined.expressionNames['#attr_status'] == 'status'
			combined.expressionNames['#attr_amount'] == 'amount'
			combined.expressionNames['#attr_threshold'] == 'threshold'
			combined.expressionValues.size() == 1
			combined.expressionValues[':val_status'].s() == 'ACTIVE'
	} // }}}

	def "Should be able to create complex filters with attribute comparisons"() { // {{{
		when:
			def filter = match('active', true).and (
				attributeGreaterThan('currentValue', 'minValue'),
				attributeLessThan('currentValue', 'maxValue')
			)

		then:
			filter.expression == '#attr_active = :val_active AND #attr_currentValue > #attr_minValue AND #attr_currentValue < #attr_maxValue'
			filter.expression.contains('#attr_currentValue > #attr_minValue')
			filter.expression.contains('#attr_currentValue < #attr_maxValue')
			filter.expressionNames.size() == 4
			filter.expressionValues.size() == 1
	} // }}}

	def "Should create IN filter for string values"() { // {{{
		when:
			def filter = in('status', 'ACTIVE', 'PENDING', 'PROCESSING')

		then:
			filter.expression == '#attr_status IN (:val_status_0, :val_status_1, :val_status_2)'
			filter.expressionNames == ['#attr_status': 'status']
			filter.expressionValues.size() == 3
			filter.expressionValues[':val_status_0'].s() == 'ACTIVE'
			filter.expressionValues[':val_status_1'].s() == 'PENDING'
			filter.expressionValues[':val_status_2'].s() == 'PROCESSING'
	} // }}}

	def "Should create IN filter for numeric values"() { // {{{
		when:
			def filter = in('priority', 1, 2, 3)

		then:
			filter.expression == '#attr_priority IN (:val_priority_0, :val_priority_1, :val_priority_2)'
			filter.expressionNames == ['#attr_priority': 'priority']
			filter.expressionValues.size() == 3
			filter.expressionValues[':val_priority_0'].n() == '1'
			filter.expressionValues[':val_priority_1'].n() == '2'
			filter.expressionValues[':val_priority_2'].n() == '3'
	} // }}}

	def "Should combine IN filter with others"() { // {{{
		when:
			def filter = in('status', 'ACTIVE', 'PENDING')
				.and(greater('priority', 5))

		then:
			filter.expression == '#attr_status IN (:val_status_0, :val_status_1) AND #attr_priority > :val_priority'
			filter.expressionNames.size() == 2
			filter.expressionValues.size() == 3
	} // }}}

	def "Should be able to create BETWEEN filter for string values"() { // {{{
		when:
			def filter = between('name', 'A', 'M')

		then:
			filter.expression == '#attr_name BETWEEN :val_name_start AND :val_name_end'
			filter.expressionNames == ['#attr_name': 'name']
			filter.expressionValues.size() == 2
			filter.expressionValues[':val_name_start'].s() == 'A'
			filter.expressionValues[':val_name_end'].s() == 'M'
	} // }}}

	def "Should be able to create BETWEEN filter for numeric values"() { // {{{
		when:
			def filter = between('age', 18, 65)

		then:
			filter.expression == '#attr_age BETWEEN :val_age_start AND :val_age_end'
			filter.expressionNames == ['#attr_age': 'age']
			filter.expressionValues.size() == 2
			filter.expressionValues[':val_age_start'].n() == '18'
			filter.expressionValues[':val_age_end'].n() == '65'
	} // }}}

	def "Should be able to combine BETWEEN filters with others"() { // {{{
		when:
			def filter = between('price', 10, 100)
				.and(match('category', 'electronics'))

		then:
			filter.expression == '(#attr_price BETWEEN :val_price_start AND :val_price_end) AND #attr_category = :val_category'
			filter.expressionNames.size() == 2
			filter.expressionValues.size() == 3
	} // }}}

	def "Should be able to create complex filters with IN and BETWEEN"() { // {{{
		when:
			def filter = in('category', 'books', 'electronics')
				.and(between('price', 20, 200))
				.and(match('inStock', true))

		then:
			filter.expression.contains('category')
			filter.expression.contains('price BETWEEN')
			filter.expression.contains('inStock')
			filter.expressionNames.size() == 3
			filter.expressionValues.size() == 5
	} // }}}

	def "Should build complex filter expression without redundant parentheses"() {	// {{{
		when:
			def filter = greaterOrEqual('date', '2025-01-01').and (
				match('modifiedBy', 'someone'),
				isBlank('aField'),
				match('type', 'aType').not(),
				contains('anotherField', 'aValue').not(),
				between('created', '2025-02', '2025-04'),
				in('status', 'ERROR', 'OK')
			)
		and:
			def expression = filter.expression
			println "expression (${expression.getClass()}) -> ${expression}"

		then:
			!expression.contains('((')
		and:
			expression == '#attr_date >= :val_date AND #attr_modifiedBy = :val_modifiedBy AND (attribute_not_exists(#attr_aField) OR #attr_aField = :val_aField) AND (NOT #attr_type = :val_type) AND (NOT contains(#attr_anotherField, :val_anotherField)) AND (#attr_created BETWEEN :val_created_start AND :val_created_end) AND #attr_status IN (:val_status_0, :val_status_1)'

		and:
			filter.expressionNames.size() == 7
			filter.expressionValues.size() == 9
	} // }}}
}
// vim: fdm=marker
