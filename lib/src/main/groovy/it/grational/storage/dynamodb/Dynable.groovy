package it.grational.storage.dynamodb

// imports {{{
import groovy.util.logging.Slf4j
import groovy.transform.ToString
import groovy.transform.CompileStatic
import it.grational.storage.DbMapper
import it.grational.storage.Storable
import static it.grational.storage.dynamodb.FieldType.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
// }}}

@Slf4j
@CompileStatic
@ToString (
	includeNames=true,
	includePackage=false,
	includeFields=true,
	includes=['v']
)
abstract class Dynable
	implements Storable<AttributeValue,Object> {
	protected Integer v = 0

	protected Dynable() {}

	protected Dynable (
		Map<String, Object> builder
	) {
		log.debug('Dynable constructor: {}', builder)
		loadVersion(builder)
	}

	protected void loadVersion (
		Map<String, Object> builder
	) {
		if ( builder?.containsKey('v') ) {
			v = builder.get('v') as Integer
		}
	}

	@Override
	final DbMapper<AttributeValue,Object> impress (
		DbMapper<AttributeValue,Object> mapper,
		boolean versioned = true
	) {
		return (
			inpress(mapper as DynamoMapper) as DynamoMapper
		).with ('v', v,
			versioned ? VERSION : STANDARD
		)
	}

	protected abstract DbMapper<AttributeValue,Object> inpress (
		DynamoMapper mapper
	)

	abstract KeyFilter key()
}
