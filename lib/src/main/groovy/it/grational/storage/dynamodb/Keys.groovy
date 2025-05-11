package it.grational.storage.dynamodb

/**
 * Represents a DynamoDB key schema definition.
 *
 * Keys provides a way to define and manage DynamoDB key schemas for tables and indexes.
 * A Keys object consists of a partition key (required) and an optional sort key.
 *
 * Example usage:
 * <pre>
 * // Create keys with string parameters
 * Keys tableKeys = Keys.of("userId", "timestamp")
 *
 * // Create keys with custom Scalar types
 * Keys customKeys = Keys.of(
 *   Scalar.of("userId", ScalarAttributeType.S),
 *   Scalar.of("timestamp", ScalarAttributeType.N)
 * )
 * </pre>
 */
class Keys {
	/** The partition key (required) */
	Scalar partition

	/** The optional sort key */
	Optional<Scalar> sort

	/**
	 * Private constructor for Keys objects
	 *
	 * @param partition The partition key (hash key)
	 * @param sort The optional sort key (range key)
	 */
	private Keys (
		Scalar partition,
		Optional<Scalar> sort
	) {
		this.partition = partition
		this.sort = sort
	}

	/**
	 * Creates a Keys object from string attribute names
	 *
	 * @param partition The partition key attribute name
	 * @param sort The optional sort key attribute name (can be null)
	 * @return A new Keys object
	 */
	static Keys of (
		String partition,
		String sort = null
	) {
		Scalar spart = Scalar.of(partition)
		Optional<Scalar> osort = sort ?
			Optional.of(Scalar.of(sort)) :
			Optional.empty()
		new Keys (
			spart,
			osort
		)
	}

	/**
	 * Creates a Keys object from Scalar objects
	 *
	 * @param partition The partition key as a Scalar
	 * @param sort The optional sort key as a Scalar (can be null)
	 * @return A new Keys object
	 */
	static Keys of (
		Scalar partition,
		Scalar sort = null
	) {
		Optional<Scalar> osort = sort ?
			Optional.of(sort) :
			Optional.empty()
		new Keys (
			partition,
			osort
		)
	}

	/**
	 * Gets all key attributes as a list
	 *
	 * @return A list containing the partition key and sort key (if present)
	 */
	List<Scalar> attributes() {
		List<Scalar> scalars = [ partition ]
		if (sort.isPresent())
			scalars << sort.get()
		return scalars
	}

	/**
	 * Gets the partition key
	 *
	 * @return The partition key Scalar
	 */
	Scalar getPartition() {
		return partition
	}

	/**
	 * Gets the optional sort key
	 *
	 * @return An Optional containing the sort key Scalar, or empty if no sort key
	 */
	Optional<Scalar> getSort() {
		return sort
	}

}
