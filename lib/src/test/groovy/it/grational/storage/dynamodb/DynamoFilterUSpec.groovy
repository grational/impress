package it.grational.storage.dynamodb

// imports {{{
import spock.lang.*
import static it.grational.storage.dynamodb.DynamoFilter.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

class DynamoFilterUSpec extends Specification {

	def "Should be able to create 'equals' filters for string values"() { // {{{
		when:
			def filter = equals('status', 'ACTIVE')

		then:
			filter.expression == '#attr_status = :val_status'
			filter.expressionNames == ['#attr_status': 'status']
			filter.expressionValues[':val_status'].s() == 'ACTIVE'
	} // }}}

	def "Should be able to create 'equals' filters for numeric value"() { // {{{
		when:
			def filter = equals('amount', 100)

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
			def filter1 = equals('status', 'ACTIVE')
			def filter2 = compare('price', '>', 100)

		when:
			def combined = filter1.and(filter2)

		then:
			combined.expression == '(#attr_status = :val_status) AND (#attr_price > :val_price)'
			combined.expressionNames.size() == 2
			combined.expressionValues.size() == 2
	} // }}}

	def "Should be able to combine filters with the OR operator"() { // {{{
		given:
			def filter1 = equals('type', 'PREMIUM')
			def filter2 = compare('credits', '>=', 1000)

		when:
			def combined = filter1.or(filter2)

		then:
			combined.expression == '(#attr_type = :val_type) OR (#attr_credits >= :val_credits)'
			combined.expressionNames.size() == 2
			combined.expressionValues.size() == 2
	} // }}}

	def "Should be able to negate a filter"() { // {{{
		given:
			def filter = equals('status', 'DELETED')

		when:
			def negated = filter.not()

		then:
			negated.expression == 'NOT (#attr_status = :val_status)'
			negated.expressionNames.size() == 1
			negated.expressionValues.size() == 1
	} // }}}

	def "Should be able to combine multiple filters"() { // {{{
		given:
			def statusFilter = equals('status', 'ACTIVE')
			def typeFilter = equals('type', 'PREMIUM')
			def priceFilter = compare('price', '>=', 50)
			def keywordFilter = contains('description', 'special')

		when:
			def complex = statusFilter
				.and(typeFilter)
				.and(priceFilter.or(keywordFilter).not())

		then:
			complex.expression == '((#attr_status = :val_status) AND (#attr_type = :val_type)) AND (NOT ((#attr_price >= :val_price) OR (contains(#attr_description, :val_description))))'
			complex.expressionNames.size() == 4
			complex.expressionValues.size() == 4
	} // }}}

	def "Should be able to create equals filters for boolean values"() { // {{{
		when:
			def filter = equals('active', true)

		then:
			filter.expression == '#attr_active = :val_active'
			filter.expressionNames == ['#attr_active': 'active']
			filter.expressionValues[':val_active'].bool() == true

		when:
			def falseFilter = equals('canceled', false)

		then:
			falseFilter.expressionValues[':val_canceled'].bool() == false
	} // }}}

	def "Should create a filter for checking if an attribute is blank"() { // {{{
		when:
			def filter = isBlank("status")

		then:
			filter.expression == "attribute_not_exists(#attr_status) OR #attr_status = :val_status"
			filter.expressionNames == ["#attr_status": "status"]
			filter.expressionValues.containsKey(":val_status")
			filter.expressionValues[":val_status"].nul()
	} // }}}

	def "Should create a filter for checking if an attribute is not blank"() { // {{{
		when:
			def filter = isNotBlank("status")

		then:
			filter.expression == "attribute_exists(#attr_status) AND NOT #attr_status = :val_status"
			filter.expressionNames == ["#attr_status": "status"]
			filter.expressionValues.containsKey(":val_status")
			filter.expressionValues[":val_status"].nul()
	} // }}}

	def "Should be able to combine isBlank with other filters"() { // {{{
		given:
			def blankFilter = isBlank("status")
			def equalsFilter = equals("category", "active")

		when:
			def combined = blankFilter.and(equalsFilter)

		then:
			combined.expression.contains("attribute_not_exists")
			combined.expression.contains("#attr_category = :val_category")
			combined.expressionNames.size() == 2
			combined.expressionValues.size() == 2
	} // }}}

	def "Should be able to negate blank check filters"() { // {{{
		when:
			def filter = isBlank("status").not()

		then:
			filter.expression == "NOT (attribute_not_exists(#attr_status) OR #attr_status = :val_status)"
	} // }}}

	def "Should maintain distinct values when filtering on the same attribute with different values"() { // {{{
		given:
			def filter1 = equals('v', 1)
			def filter2 = equals('v', 2)

		when:
			def combined = filter1.or(filter2)

		then:
			!combined.expression.contains('= :val_v OR #attr_v = :val_v')
			combined.expression.contains('= :val_v_')
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
			def statusActive = equals('status', 'ACTIVE')
			def statusPending = equals('status', 'PENDING')
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
			def filter = greaterThanEquals("price", 10)
				.and(lessThanEquals("price", 100))
				
		then:
			filter.expression.contains(">= :val_price")
			filter.expression.contains("<= :val_price")
			filter.expressionNames.size() == 1  
			filter.expressionValues.size() == 2
	} // }}}
	
	def "Should handle compound filter expressions"() { // {{{
		when:
			def activeProducts = equals("type", "product")
				.and(equals("status", "active"))
			
			def featuredProducts = equals("type", "product")
				.and(equals("featured", true))
				
			def filter = activeProducts.or(featuredProducts)
				
		then:
			filter.expression.contains("type")
			filter.expression.contains("status")
			filter.expression.contains("featured")
			filter.expression.contains("OR")
			filter.expression.contains("AND")
			filter.expressionNames.size() == 3
	} // }}}
	
	def "Should handle complex date range filters"() { // {{{
		when:
			def dateFilter = greaterThanEquals("created_at", 20250101)
				.and(lessThan("created_at", 20250201))
				.and(isBlank("deleted_at"))
				
		then:
			dateFilter.expression.contains(">= :val_created_at")
			dateFilter.expression.contains("< :val_created_at")
			dateFilter.expression.contains("attribute_not_exists(#attr_deleted_at)")
			dateFilter.expressionNames.size() == 2
			dateFilter.expressionValues.size() == 3
	} // }}}
	
	def "Should support multiple comparison filters on different fields"() { // {{{
		when:
			def filter = greaterThan("priority", 5)
				.and(lessThan("price", 100))
				.and(equals("category", "electronics"))
				
		then:
			filter.expression.contains("> :val_priority")
			filter.expression.contains("< :val_price")
			filter.expression.contains("= :val_category")
			filter.expressionNames.size() == 3
			filter.expressionValues.size() == 3
	} // }}}
	
	def "Should create OR conditions for value ranges"() { // {{{
		when:
			def lowRange = lessThan("price", 10)
			def highRange = greaterThan("price", 100)
			def filter = lowRange.or(highRange)
				
		then:
			filter.expression == "(#attr_price < :val_price) OR (#attr_price > :val_price_1)"
			filter.expressionNames.size() == 1
			filter.expressionValues.size() == 2
			filter.expressionValues.containsKey(":val_price")
			filter.expressionValues.containsKey(":val_price_1")
	} // }}}
	
	def "Should correctly combine filters with NOT operations"() { // {{{
		when:
			def filter = equals("status", "ACTIVE").and (
				lessThanEquals("price", 1000).not()
			)
				
		then:
			filter.expression.contains("= :val_status")
			filter.expression.contains("NOT (#attr_price <= :val_price)")
			filter.expressionNames.size() == 2
			filter.expressionValues.size() == 2
	} // }}}
}
// vim: fdm=marker
