package it.grational.storage.dynamodb

/**
 * Represents a DynamoDB secondary index definition.
 *
 * An Index combines key schema (partition key and optional sort key) with an index name.
 * This class is used primarily for creating tables with secondary indexes.
 *
 * Example usage:
 * <pre>
 * // Simple index with auto-generated name
 * Index emailIndex = Index.of("email")  // name will be "email-index"
 *
 * // Composite index with custom name
 * Index customIndex = Index.of("status", "createdAt", "status-date-index")
 *
 * // Create index from Keys object
 * Keys keys = Keys.of("email", "username")
 * Index combinedIndex = new Index(keys, "email-username-index")
 * </pre>
 */
class Index {
	/**
	 * The key schema for this index, defining partition and sort keys.
	 * Delegates methods to Keys class.
	 */
	@Delegate
	Keys keys

	/**
	 * The name of the index
	 */
	String name

	/**
	 * Creates an Index from Keys and index name
	 *
	 * @param keys The Keys object defining the key schema
	 * @param name The index name
	 */
	private Index (
		Keys keys,
		String name
	) {
		this.keys = keys
		this.name = name
	}

	/**
	 * Creates an Index from string attribute names
	 *
	 * @param partition The partition key attribute name
	 * @param sort The optional sort key attribute name (can be null)
	 * @param name The optional index name (if null, an auto-generated name will be used)
	 * @return A new Index object
	 */
	static Index of (
		String partition,
		String sort = null,
		String name = null
	) {
		String safe = name ?: autoname(partition, sort)
		new Index (
			Keys.of(partition, sort),
			safe
		)
	}

	/**
	 * Creates an Index from Scalar objects
	 *
	 * @param partition The partition key as a Scalar
	 * @param sort The optional sort key as a Scalar (can be null)
	 * @param name The optional index name (if null, an auto-generated name will be used)
	 * @return A new Index object
	 */
	static Index of (
		Scalar partition,
		Scalar sort = null,
		String name = null
	) {
		String safe = name ?: autoname (
			partition.name, sort ? sort.name : null
		)
		new Index (
			Keys.of(partition, sort),
			safe
		)
	}

	/**
	 * Generates an index name if one is not provided
	 *
	 * @param partition The partition key name
	 * @param sort The optional sort key name
	 * @return A generated index name in the format "partition[-sort]-index"
	 */
	private static String autoname (
		String partition,
		String sort
	) {
		StringBuilder sb = new StringBuilder()
		sb.append(partition)
		if ( sort )
			sb.append("-").append(sort)
		sb.append("-index")
		return sb.toString()
	}

	/**
	 * Gets all index key attributes as a list
	 *
	 * @return A list containing the partition key and sort key (if present)
	 */
	List<Scalar> attributes() {
		List<Scalar> scalars = [ partition ]
		if (sort.isPresent())
			scalars << sort.get()
		return scalars
	}
}
