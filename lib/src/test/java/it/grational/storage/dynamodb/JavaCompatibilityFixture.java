package it.grational.storage.dynamodb;

import it.grational.storage.DbMapper;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class JavaCompatibilityFixture {
	private JavaCompatibilityFixture() {}

	public static final class JavaBean implements DynamoStorable {
		private String id;
		private String name;
		private int score;

		public JavaBean() {}

		public JavaBean(String id, String name, int score) {
			this.id = id;
			this.name = name;
			this.score = score;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getScore() {
			return score;
		}

		public void setScore(int score) {
			this.score = score;
		}

		@Override
		public DynamoDbMapper impress(
			DynamoDbMapper mapper,
			boolean versioned
		) {
			return mapper
				.with("id", id, FieldType.PARTITION_KEY)
				.with("name", name)
				.with("score", score);
		}
	}

	public record JavaRecord(
		String id,
		String name,
		long score
	) implements DynamoStorable {
		@Override
		public DynamoDbMapper impress(
			DynamoDbMapper mapper,
			boolean versioned
		) {
			return mapper
				.with("id", id, FieldType.PARTITION_KEY)
				.with("name", name)
				.with("score", score);
		}
	}

	public static DynamoDbMapper chainMapper(DynamoDbMapper mapper) {
		return mapper
			.with("id", "user-1", FieldType.PARTITION_KEY)
			.with("name", "Ada")
			.with("age", 42)
			.with("active", true)
			.withNull("nickname")
			.remove("obsolete");
	}

	public static DynamoDbMapper addCollections(
		DynamoDbMapper mapper,
		List<JavaBean> beans,
		List<DynamoMapper> mappers
	) {
		return mapper
			.withItems("beans", beans)
			.withMappers("mappers", mappers);
	}

	public static DynamoDbMapper addNestedItem(
		DynamoDbMapper mapper,
		JavaBean bean
	) {
		return mapper.withItem("bean", bean);
	}

	public static Map<String, Object> asMap(DynamoMap map) {
		map.put("fromJava", true);
		return map;
	}

	public static KeyFilter keyFilter() {
		return KeyFilter
			.partition("id", "user-1")
			.sort("createdAt", 10)
			.build();
	}

	public static DbMapper<AttributeValue, Object> genericMapper(
		JavaBean bean
	) {
		return bean.impress(new DynamoMapper(), true);
	}
}
