package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter

class DynamoDbCreateTableUSpec extends Specification {

	def "Should create table with on demand billing by default"() {
		given:
			DynamoDbClient client = Mock()
			DynamoDbWaiter waiter = Mock()
			DynamoDb dynamo = new DynamoDb(client)
			CreateTableRequest request

		when:
			dynamo.createTable (
				'billing_default',
				'id'
			)

		then:
			1 * client.describeTable(_ as DescribeTableRequest) >> {
				throw ResourceNotFoundException.builder().build()
			}
			1 * client.createTable(_ as CreateTableRequest) >> {
				CreateTableRequest captured ->
					request = captured
					CreateTableResponse.builder().build()
			}
			1 * client.waiter() >> waiter
			1 * waiter.waitUntilTableExists(_ as DescribeTableRequest) >> null

		and:
			request.billingMode() == BillingMode.PAY_PER_REQUEST
			request.provisionedThroughput() == null
	}

	def "Should create table with provisioned billing"() {
		given:
			DynamoDbClient client = Mock()
			DynamoDbWaiter waiter = Mock()
			DynamoDb dynamo = new DynamoDb(client)
			CreateTableRequest request

		when:
			dynamo.createTable (
				'billing_provisioned',
				'id',
				BillingOptions.provisioned(3, 4)
			)

		then:
			1 * client.describeTable(_ as DescribeTableRequest) >> {
				throw ResourceNotFoundException.builder().build()
			}
			1 * client.createTable(_ as CreateTableRequest) >> {
				CreateTableRequest captured ->
					request = captured
					CreateTableResponse.builder().build()
			}
			1 * client.waiter() >> waiter
			1 * waiter.waitUntilTableExists(_ as DescribeTableRequest) >> null

		and:
			request.billingMode() == BillingMode.PROVISIONED
			request.provisionedThroughput().readCapacityUnits() == 3
			request.provisionedThroughput().writeCapacityUnits() == 4
	}

	def "Should create indexed table with provisioned index billing"() {
		given:
			DynamoDbClient client = Mock()
			DynamoDbWaiter waiter = Mock()
			DynamoDb dynamo = new DynamoDb(client)
			CreateTableRequest request

		when:
			dynamo.createTable (
				'billing_indexed',
				'id',
				[
					Index.of('email')
				] as Index[],
				BillingOptions.provisioned(3, 4, 5, 6)
			)

		then:
			1 * client.describeTable(_ as DescribeTableRequest) >> {
				throw ResourceNotFoundException.builder().build()
			}
			1 * client.createTable(_ as CreateTableRequest) >> {
				CreateTableRequest captured ->
					request = captured
					CreateTableResponse.builder().build()
			}
			1 * client.waiter() >> waiter
			1 * waiter.waitUntilTableExists(_ as DescribeTableRequest) >> null

		and:
			request.billingMode() == BillingMode.PROVISIONED
			request.provisionedThroughput().readCapacityUnits() == 3
			request.provisionedThroughput().writeCapacityUnits() == 4
			request.globalSecondaryIndexes().size() == 1
			request.globalSecondaryIndexes()[0]
				.provisionedThroughput()
				.readCapacityUnits() == 5
			request.globalSecondaryIndexes()[0]
				.provisionedThroughput()
				.writeCapacityUnits() == 6
	}
}
