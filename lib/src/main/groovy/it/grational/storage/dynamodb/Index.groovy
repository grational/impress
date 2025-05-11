package it.grational.storage.dynamodb

class Index {
	@Delegate
	Keys keys
	String name

	private Index (
		Keys keys,
		String name
	) {
		this.keys = keys
		this.name = name
	}

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

	List<Scalar> attributes() {
		List<Scalar> scalars = [ partition ]
		if (sort.isPresent())
			scalars << sort.get()
		return scalars
	}
}
