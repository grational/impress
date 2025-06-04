package it.grational.storage.dynamodb

// imports {{{
import spock.lang.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import static it.grational.storage.dynamodb.DynamoFilter.*
// }}}

/**
 * Integration test to verify nested field support with actual DynamoDB operations.
 */
class DynamoNestedFieldsICSpec extends Specification {

	// members {{{
	@Shared
	DynamoDb dynamo
	@Shared
	URI endpoint = 'http://localhost:8888'.toURI()
	@Shared
	String tableName = 'TestNestedFieldsTable'
	// }}}

	def setupSpec() { // {{{
		dynamo = new DynamoDb (
			DynamoDbClient.builder()
			.endpointOverride(endpoint)
			.build()
		)

		// Create test table
		dynamo.createTable(tableName, 'id')

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
			dynamo.putItem(tableName, item)
		}
	} // }}}

	def cleanupSpec() { // {{{
		dynamo.dropTable(tableName)
	} // }}}

	def "Should filter items by nested field equals condition"() { // {{{
		when:
			
			def results = dynamo
			.scan(tableName)
			.filter(match('user.profile.active', true))
			.list()

		then:
			results.size() == 2
			results.every { it.user.profile.active }
			results*.id ==~ ['user1', 'user2']
	} // }}}

	def "Should filter items by nested field greater than condition"() { // {{{
		when:
			def results = dynamo
			.scan(tableName)
			.filter(greater('user.profile.score', 70))
			.list()

		then:
			results.size() == 2
			results.every { it.user.profile.score > 70 }
			results*.id ==~ ['user1', 'user2']
	} // }}}

	def "Should filter items by nested field contains condition"() { // {{{
		when:
			def results = dynamo
			.scan(tableName)
			.filter(contains('user.profile.tags', 'premium'))
			.list()

		then:
			results.size() == 1
			results[0].id == 'user1'
	} // }}}

	def "Should support complex filters with multiple nested fields"() { // {{{
		when:
			def results = dynamo.scan(tableName)
			.filter (
				every (
					match('user.profile.active', true),
					greater('user.profile.score', 85)
				)
			).list()

		then:
			results.size() == 1
			results[0].id == 'user1'
			results[0].user.profile.active == true
			results[0].user.profile.score >= 85
	} // }}}

	def "Should query items using nested field key conditions"() { // {{{
		when:
			def results = dynamo
			.scan(tableName)
			.filter(match('user.address.city', 'New York'))
			.list()

		then:
			results.size() == 1
			results[0].id == 'user2'
			results[0].user.address.city == 'New York'
	} // }}}

}
// vim: fdm=marker
