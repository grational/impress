package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.BillingMode

class BillingOptionsUSpec extends Specification {

	def "Should create on demand billing options"() {
		when:
			def billing = BillingOptions.onDemand()

		then:
			billing.mode == BillingMode.PAY_PER_REQUEST
			billing.tableThroughput == Optional.empty()
			billing.indexThroughput == Optional.empty()
	}

	def "Should create provisioned billing options"() {
		when:
			def billing = BillingOptions.provisioned (
				3,
				4,
				5,
				6
			)

		then:
			billing.mode == BillingMode.PROVISIONED
			billing.tableThroughput.get().readCapacityUnits() == 3
			billing.tableThroughput.get().writeCapacityUnits() == 4
			billing.indexThroughput.get().readCapacityUnits() == 5
			billing.indexThroughput.get().writeCapacityUnits() == 6
	}

	def "Should reject invalid provisioned throughput"() {
		when:
			BillingOptions.provisioned (
				readCapacityUnits,
				writeCapacityUnits
			)

		then:
			thrown(IllegalArgumentException)

		where:
			readCapacityUnits | writeCapacityUnits
			0                 | 1
			1                 | 0
			-1                | 1
			1                 | -1
	}
}
