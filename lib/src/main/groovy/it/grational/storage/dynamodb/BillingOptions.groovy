package it.grational.storage.dynamodb

import groovy.transform.CompileStatic
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput

/**
 * Billing configuration for DynamoDB table creation.
 */
@CompileStatic
class BillingOptions {
	final BillingMode mode
	final Optional<ProvisionedThroughput> tableThroughput
	final Optional<ProvisionedThroughput> indexThroughput

	private BillingOptions (
		BillingMode mode,
		Optional<ProvisionedThroughput> tableThroughput,
		Optional<ProvisionedThroughput> indexThroughput
	) {
		this.mode = mode
		this.tableThroughput = tableThroughput
		this.indexThroughput = indexThroughput
	}

	static BillingOptions onDemand() {
		new BillingOptions (
			BillingMode.PAY_PER_REQUEST,
			Optional.empty(),
			Optional.empty()
		)
	}

	static BillingOptions provisioned (
		long readCapacityUnits,
		long writeCapacityUnits
	) {
		provisioned (
			readCapacityUnits,
			writeCapacityUnits,
			readCapacityUnits,
			writeCapacityUnits
		)
	}

	static BillingOptions provisioned (
		long tableReadCapacityUnits,
		long tableWriteCapacityUnits,
		long indexReadCapacityUnits,
		long indexWriteCapacityUnits
	) {
		new BillingOptions (
			BillingMode.PROVISIONED,
			Optional.of (
				throughput (
					tableReadCapacityUnits,
					tableWriteCapacityUnits
				)
			),
			Optional.of (
				throughput (
					indexReadCapacityUnits,
					indexWriteCapacityUnits
				)
			)
		)
	}

	void applyTo (
		CreateTableRequest.Builder builder
	) {
		builder.billingMode(mode)

		tableThroughput.ifPresent {
			ProvisionedThroughput throughput ->
				builder.provisionedThroughput(throughput)
		}
	}

	void applyTo (
		GlobalSecondaryIndex.Builder builder
	) {
		indexThroughput.ifPresent {
			ProvisionedThroughput throughput ->
				builder.provisionedThroughput(throughput)
		}
	}

	private static ProvisionedThroughput throughput (
		long readCapacityUnits,
		long writeCapacityUnits
	) {
		if (readCapacityUnits < 1)
			throw new IllegalArgumentException (
				'readCapacityUnits must be greater than zero'
			)

		if (writeCapacityUnits < 1)
			throw new IllegalArgumentException (
				'writeCapacityUnits must be greater than zero'
			)

		ProvisionedThroughput.builder()
			.readCapacityUnits(readCapacityUnits)
			.writeCapacityUnits(writeCapacityUnits)
			.build()
	}
}
