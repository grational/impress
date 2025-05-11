package it.grational.storage.dynamodb

class Keys {
	Scalar partition
	Optional<Scalar> sort

	private Keys (
		Scalar partition,
		Optional<Scalar> sort
	) {
		this.partition = partition
		this.sort = sort
	}

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

	List<Scalar> attributes() {
		List<Scalar> scalars = [ partition ]
		if (sort.isPresent())
			scalars << sort.get()
		return scalars
	}

	Scalar getPartition() {
		return partition
	}

	Optional<Scalar> getSort() {
		return sort
	}

}
