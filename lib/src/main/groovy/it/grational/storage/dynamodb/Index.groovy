package it.grational.storage.dynamodb

class Index {
	Scalar partition
	Optional<Scalar> sort
	String name

	private Index (
		Scalar partition,
		Optional<Scalar> sort,
		String name
	) {
		this.partition = partition
		this.sort = sort
		this.name = name
	}

	static of(Scalar partition) {
		new Index (
			partition,
			Optional.empty(),
			"${partition.name}-index"
		)
	}

	static of (
		Scalar partition,
		Scalar sort
	) {
		new Index (
			partition,
			Optional.of(sort),
			autoname(partition, sort)
		)
	}

	static of (
		Scalar partition,
		Scalar sort,
		String name
	) {
		Optional<Scalar> osort = optionalize(sort)
		String safe = name ?: autoname(partition, osort)
		new Index (
			partition,
			osort,
			safe
		)
	}

	private static Optional<Scalar> optionalize(Scalar value) {
		( value ) ? Optional.of(value) : Optional.empty()
	}

	private static String autoname (
		Scalar partition,
		Optional<Scalar> sort
	) {
		StringBuilder sb = new StringBuilder()
		sb.append(partition.name)
		if ( sort.isPresent() )
			sb.append("-").append(sort.get().name)
		sb.append("-index")
		return sb.toString()
	}

	List<Scalar> attributes() {
		List<Scalar> scalars = [ partition ]
		if (sort.isPresent())
			scalars << sort.get()
		return scalars
	}
}
