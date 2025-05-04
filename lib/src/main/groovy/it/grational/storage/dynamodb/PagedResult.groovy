package it.grational.storage.dynamodb

// imports {{{
import groovy.transform.ToString
import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

/**
 * A wrapper class with both the query results and pagination info
 */
@ToString (
	includePackage = false,
	includeFields = true,
	includeNames = true
)
@CompileStatic
class PagedResult<T> {
	/**
	 * The list of items returned from the query
	 */
	final List<T> items

	/**
	 * The last evaluated key used for pagination for the next query
	 * Null if there are no more results.
	 */
	final Map<String, AttributeValue> last

	/**
	 * Whether there are more results available
	 */
	final boolean more

	/**
	 * The number of items returned in this page
	 */
	final int count

	/**
	 * Construct a new PagedResult
	 * 
	 * @param items The list of items returned from the query
	 * @param last The last evaluated key for pagination
	 */
	PagedResult (
		List<T> items,
		Map<String, AttributeValue> last
	) {
		this.items = items ?: []
		this.last = last
		this.more = !!last
		this.count = this.items.size()
	}

}
