package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*

/**
 * Integration test to verify nested field support with actual DynamoDB operations.
 */
class DynamoNestedFieldsICSpec extends Specification {

	@Shared
	DynamoDb dynamoDb
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()
	@Shared
	String tableName = 'TestNestedFieldsTable'

	def setupSpec() {
		dynamoDb = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)

		// Create test table
		dynamoDb.createTable(tableName, 'id')

		// Insert test items with nested attributes
		def items = [
			new DynamoMap (
				id: 'user1',
				user: [
					name: 'John Doe',
					profile: [
						active: true,
						score: 95,
						tags: ['premium', 'verified']
					],
					address: [
						street: '123 Main St',
						city: 'San Francisco',
						zipcode: '94105'
					]
				]
			),
			new DynamoMap (
				id: 'user2',
				user: [
					name: 'Jane Smith',
					profile: [
						active: true,
						score: 80,
						tags: ['basic']
					],
					address: [
						street: '456 Market St',
						city: 'New York',
						zipcode: '10001'
					]
				]
			),
			new DynamoMap (
				id: 'user3',
				user: [
					name: 'Bob Johnson',
					profile: [
						active: false,
						score: 60,
						tags: []
					],
					address: [
						street: '789 Oak St',
						city: 'Chicago',
						zipcode: '60601'
					]
				]
			)
		]

		items.each { item ->
			dynamoDb.putItem(tableName, item)
		}
	}

	def cleanupSpec() {
		dynamoDb.dropTable(tableName)
	}

	def "Should filter items by nested field equals condition"() {
		when:
			def filter = DynamoFilter.match('user.profile.active', true)
			def results = dynamoDb.scan(tableName, filter)
			println "results (${results.getClass()}) -> ${results}"

		then:
			results.size() == 2
			results.every { it.user.profile.active }
			results*.id ==~ ['user1', 'user2']
	}

	def "Should filter items by nested field greater than condition"() {
		when:
			def filter = DynamoFilter.greater('user.profile.score', 70)
			def results = dynamoDb.scan(tableName, filter)

		then:
			results.size() == 2
			results.every { it.user.profile.score > 70 }
			results*.id ==~ ['user1', 'user2']
	}

	def "Should filter items by nested field contains condition"() {
		when:
			def filter = DynamoFilter.contains('user.profile.tags', 'premium')
			def results = dynamoDb.scan(tableName, filter)

		then:
			results.size() == 1
			results[0].id == 'user1'
	}

	def "Should support complex filters with multiple nested fields"() {
		when:
			def filter = DynamoFilter.match('user.profile.active', true)
				.and(DynamoFilter.greater('user.profile.score', 85))

			def results = dynamoDb.scan(tableName, filter)

		then:
			results.size() == 1
			results[0].id == 'user1'
			results[0].user.profile.active == true
			results[0].user.profile.score >= 85
	}

	def "Should query items using nested field key conditions"() {
		given:
			// For querying by nested fields, we'd typically use a Global Secondary Index
			// in a real application. For this test, we'll use a scan with filter.
			def filter = DynamoFilter.match('user.address.city', 'New York')

		when:
			def results = dynamoDb.scan(tableName, filter)

		then:
			results.size() == 1
			results[0].id == 'user2'
			results[0].user.address.city == 'New York'
	}
}
