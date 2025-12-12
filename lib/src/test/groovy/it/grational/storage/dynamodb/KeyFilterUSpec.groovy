package it.grational.storage.dynamodb

// imports {{{
import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import software.amazon.awssdk.core.SdkBytes
import static software.amazon.awssdk.core.SdkBytes.*
import static it.grational.storage.dynamodb.DynamoFilter.*
// }}}

class KeyFilterUSpec extends Specification {

	@Shared
	SdkBytes sdkBytes

	def setup() { // {{{
		sdkBytes = fromUtf8String('binary')
	} // }}}

	def "Should create valid key match via static of() method with string value"() { // {{{
		when:
			KeyFilter km = KeyFilter.of('string', 'value')

		then:
			noExceptionThrown()
		and:
			km.composite() == false
		and:
			km.toMap() == [string: fromS('value')]
	} // }}}

	def "Should create valid key match via static of() method with number value"() { // {{{
		when:
			KeyFilter km = KeyFilter.of('number', 1)

		then:
			noExceptionThrown()
		and:
			km.composite() == false
		and:
			km.toMap() == [number: fromN('1')]
	} // }}}

	def "Should create valid key match via static of() method with binary value"() { // {{{
		when:
			KeyFilter km = KeyFilter.of('binary', sdkBytes)

		then:
			noExceptionThrown()
		and:
			km.composite() == false
		and:
			km.toMap() == [binary: fromB(sdkBytes)]
	} // }}}

	def "Should create valid composite key match via static of() method"() { // {{{
		when:
			KeyFilter km = KeyFilter.of(pk, pv, sk, sv)

		then:
			noExceptionThrown()
		and:
			km.composite() == true
		and:
			km.toMap() == expected

		where:
			pk     | pv       | sk     | sv       || expected
			'part' | 'pvalue' | 'sort' | 'svalue' || [ part: fromS('pvalue'), sort: fromS('svalue') ]
			'part' | 'pvalue' | 'sort' | 2        || [ part: fromS('pvalue'), sort: fromN('2')      ]
			'part' | 1        | 'sort' | 2        || [ part: fromN('1'),      sort: fromN('2')      ]
			'part' | 1        | 'sort' | 'svalue' || [ part: fromN('1'),      sort: fromS('svalue') ]
			'part' | 'pvalue' | 'sort' | sdkBytes || [ part: fromS('pvalue'), sort: fromB(sdkBytes) ]
			'part' | 1        | 'sort' | sdkBytes || [ part: fromN('1'),      sort: fromB(sdkBytes) ]
			'part' | sdkBytes | 'sort' | 'svalue' || [ part: fromB(sdkBytes), sort: fromS('svalue') ]
			'part' | sdkBytes | 'sort' | 2        || [ part: fromB(sdkBytes), sort: fromN('2')      ]
			'part' | sdkBytes | 'sort' | sdkBytes || [ part: fromB(sdkBytes), sort: fromB(sdkBytes) ]
	} // }}}

	def "Should create key match from map via static of() method"() { // {{{
		given:
			Map<String, AttributeValue> keyMap = [string: fromS('value')]

		when:
			KeyFilter km = KeyFilter.of(keyMap)

		then:
			noExceptionThrown()
		and:
			km.toMap() == keyMap
	} // }}}

	def "Static of() and constructor should create equivalent instances"() { // {{{
		when:
			KeyFilter fromCtor = new KeyFilter(key, value)
			KeyFilter fromOf = KeyFilter.of(key, value)

		then:
			fromCtor == fromOf
		and:
			fromCtor.toMap() == fromOf.toMap()

		where:
			key      | value
			'string' | 'value'
			'number' | 1
			'binary' | sdkBytes
	} // }}}

	def "Static of() and constructor should create equivalent composite instances"() { // {{{
		when:
			KeyFilter fromCtor = new KeyFilter(pk, pv, sk, sv)
			KeyFilter fromOf = KeyFilter.of(pk, pv, sk, sv)

		then:
			fromCtor == fromOf
		and:
			fromCtor.toMap() == fromOf.toMap()

		where:
			pk     | pv       | sk     | sv
			'part' | 'pvalue' | 'sort' | 'svalue'
			'part' | 1        | 'sort' | 2
			'part' | sdkBytes | 'sort' | 'svalue'
	} // }}}

	def "Should return a valid dynamo key match given each supported type"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(key, value)

		then:
			noExceptionThrown()
		and:
			dk.composite() == false
		and:
			dk.toMap() == expected

		where:
			key      | value                    || expected
			'string' | 'value'                  || [string: fromS('value')]
			'number' | 1                        || [number: fromN('1')]
			'binary' | fromUtf8String('binary') || [binary: fromB(fromUtf8String('binary'))]
	} // }}}

	def "Should return a valid dynamo key also when a combined key is used"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(pk, pv, sk, sv)

		then:
			noExceptionThrown()
		and:
			dk.composite() == true
		and:
			dk.toMap() == expected

		where:
			pk     | pv       | sk     | sv       || expected
			'part' | 'pvalue' | 'sort' | 'svalue' || [ part: fromS('pvalue'), sort: fromS('svalue') ]
			'part' | 'pvalue' | 'sort' | 2        || [ part: fromS('pvalue'), sort: fromN('2')      ]
			'part' | 1        | 'sort' | 2        || [ part: fromN('1'),      sort: fromN('2')      ]
			'part' | 1        | 'sort' | 'svalue' || [ part: fromN('1'),      sort: fromS('svalue') ]
	} // }}}

	def "Should return a valid dynamo key with binary data in various composite key combinations"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(pk, pv, sk, sv)

		then:
			noExceptionThrown()
		and:
			dk.composite() == true
		and:
			dk.toMap() == expected

		where:
			pk     | pv       | sk     | sv       || expected
			'part' | 'pvalue' | 'sort' | sdkBytes || [ part: fromS('pvalue'), sort: fromB(sdkBytes) ]
			'part' | 1        | 'sort' | sdkBytes || [ part: fromN('1'),      sort: fromB(sdkBytes) ]
			'part' | sdkBytes | 'sort' | 'svalue' || [ part: fromB(sdkBytes), sort: fromS('svalue') ]
			'part' | sdkBytes | 'sort' | 2        || [ part: fromB(sdkBytes), sort: fromN('2')      ]
			'part' | sdkBytes | 'sort' | sdkBytes || [ part: fromB(sdkBytes), sort: fromB(sdkBytes) ]
	} // }}}

	def "Should be able to return the key condition expression and the relative placeholders"() { // {{{
		when:
			KeyFilter dk = new KeyFilter('string', 'value')

		then:
			dk.composite() == false

		and:
			dk.condition() == "#string = :string"
		and:
			dk.conditionNames() == [ '#string': 'string' ]
		and:
			dk.conditionValues() == [':string': fromS('value')]
	} // }}}

	def "Should be able to return the key condition expressions for composite keys"() { // {{{
		when:
			KeyFilter dk = new KeyFilter (
				'string', 'value',
				'number', 1
			)

		then:
			dk.composite() == true

		and:
			dk.condition() == "#string = :string AND #attr_number = :val_number"
		and:
			dk.conditionNames() == [
				'#string': 'string',
				'#attr_number': 'number'
			]
		and:
			dk.conditionValues() == [
				':string': fromS('value'),
				':val_number': fromN('1'),
			]
	} // }}}

	def "Should throw an exception if the key map is invalid"() { // {{{
		when:
			new KeyFilter(input)

		then:
			Exception e = thrown(IllegalArgumentException)
		and:
			e.message == exceptionMessage

		where:
			input                                             || exceptionMessage
			[:]                                               || 'Invalid key size: 0'
			[ a: fromS('a'), b: fromS('b'), one: fromN('1') ] || 'Invalid key size: 3'
			[ a: fromBool(true) ]                             || "Unsupported key types: [a:AttributeValue(BOOL=true)]"
	} // }}}

	def "Should be able to instantiate it directly with an appropriate map"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(input)

		then:
			noExceptionThrown()
		and:
			dk.composite() == expected
		and:
			dk.toMap() == input

		where:
			input                                || expected
			[ string: fromS('value') ]           || false
			[ binary: fromB(sdkBytes) ]          || false
			[ a: fromS('a'), b: fromS('b') ]     || true
			[ a: fromS('a'), one: fromN('1') ]   || true
			[ one: fromN('1'), b: fromS('b') ]   || true
			[ one: fromN('1'), two: fromN('2') ] || true
			[ bin: fromB(sdkBytes), b: fromS('b') ] || true
			[ a: fromS('a'), bin: fromB(sdkBytes) ] || true
			[ bin1: fromB(sdkBytes), bin2: fromB(sdkBytes) ] || true
	} // }}}

	def "Should be able to extract a partition key from a composite one"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(input)

		then:
			dk.partition().toMap() == expected

		where:
			input                                || expected
			[ string: fromS('value') ]           || input
			[ binary: fromB(sdkBytes) ]          || input
			[ a: fromS('a'), b: fromS('b') ]     || [ a: fromS('a') ]
			[ a: fromS('a'), one: fromN('1') ]   || [ a: fromS('a') ]
			[ one: fromN('1'), b: fromS('b') ]   || [ one: fromN('1') ]
			[ one: fromN('1'), two: fromN('2') ] || [ one: fromN('1') ]
			[ binary: fromB(sdkBytes), string: fromS('value') ] || [ binary: fromB(sdkBytes) ]
			[ a: fromS('a'), binary: fromB(sdkBytes) ] || [ a: fromS('a') ]
	} // }}}

	def "Should be able to extract a sort key from a composite one"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(input)

		then:
			dk.sort() == Optional.ofNullable(expected)

		where:
			input                                || expected
			[ string: fromS('value') ]           || null
			[ binary: fromB(sdkBytes) ]          || null
			[ a: fromS('a'), b: fromS('b') ]     || new KeyFilter(b: fromS('b'))
			[ a: fromS('a'), one: fromN('1') ]   || new KeyFilter(one: fromN('1'))
			[ one: fromN('1'), b: fromS('b') ]   || new KeyFilter(b: fromS('b'))
			[ one: fromN('1'), two: fromN('2') ] || new KeyFilter(two: fromN('1'))
			[ a: fromS('a'), bin: fromB(sdkBytes) ] || new KeyFilter(bin: fromB(sdkBytes))
			[ bin: fromB(sdkBytes), a: fromS('a') ] || new KeyFilter(a: fromS('a'))
			[ bin1: fromB(sdkBytes), bin2: fromB(sdkBytes) ] || new KeyFilter(bin2: fromB(sdkBytes))
	} // }}}

	def "Should handle nested field paths in condition expression"() { // {{{
		when:
			KeyFilter dk = new KeyFilter('user.profile.id', 'ABC123')

		then:
			dk.condition() == "#user.#profile.#id = :userprofileid"
			dk.conditionNames() == [
				'#user': 'user',
				'#profile': 'profile',
				'#id': 'id'
			]
			dk.conditionValues() == [':userprofileid': fromS('ABC123')]
	} // }}}

	def "Should handle nested field paths in composite keys"() { // {{{
		when:
			KeyFilter dk = new KeyFilter(
				'user.id', 'USER001',
				'user.group.name', 'ADMIN'
			)

		then:
			dk.condition() == "#user.#id = :userid AND #attr_user.#attr_group.#attr_name = :val_usergroupname"
			dk.conditionNames().size() == 5  // user, id, attr_user, attr_group, attr_name  
			dk.conditionNames() == [
				'#user': 'user',
				'#id': 'id',
				'#attr_user': 'user',
				'#attr_group': 'group',
				'#attr_name': 'name'
			]
			dk.conditionValues() == [
				':userid': fromS('USER001'),
				':val_usergroupname': fromS('ADMIN')
			]
	} // }}}

	def "Should handle deeply nested field paths"() { // {{{
		when:
			KeyFilter dk = new KeyFilter('data.user.profile.contact.email', 'test@example.com')

		then:
			dk.condition() == "#data.#user.#profile.#contact.#email = :datauserprofilecontactemail"
			dk.conditionNames().size() == 5
			dk.conditionNames() == [
				'#data': 'data',
				'#user': 'user',
				'#profile': 'profile',
				'#contact': 'contact',
				'#email': 'email'
			]
			dk.conditionValues() == [':datauserprofilecontactemail': fromS('test@example.com')]
	} // }}}
	
	def "Should handle special characters in path segments"() { // {{{
		when:
			KeyFilter dk = new KeyFilter('user-data.custom_field', 'value')

		then:
			dk.condition() == "#userdata.#custom_field = :userdatacustom_field"
			dk.conditionNames() == [
				'#userdata': 'user-data',
				'#custom_field': 'custom_field'
			]
			dk.conditionValues() == [':userdatacustom_field': fromS('value')]
	} // }}}

	def "Should handle binary data in condition expressions"() { // {{{
		when:
			KeyFilter dk = new KeyFilter('binary_data', sdkBytes)

		then:
			dk.condition() == "#binary_data = :binary_data"
			dk.conditionNames() == [
				'#binary_data': 'binary_data'
			]
			dk.conditionValues() == [':binary_data': fromB(sdkBytes)]
	} // }}}

	def "Should handle binary data in nested field paths"() { // {{{
		when:
			KeyFilter dk = new KeyFilter('user.profile.image', sdkBytes)

		then:
			dk.condition() == "#user.#profile.#image = :userprofileimage"
			dk.conditionNames() == [
				'#user': 'user',
				'#profile': 'profile',
				'#image': 'image'
			]
			dk.conditionValues() == [':userprofileimage': fromB(sdkBytes)]
	} // }}}

	def "Should handle binary data in composite keys with nested paths"() { // {{{
		when:
			KeyFilter dk = new KeyFilter (
				'user.id', 'USER001',
				'user.profile.image',
				sdkBytes
			)

		then:
			dk.condition() == "#user.#id = :userid AND #attr_user.#attr_profile.#attr_image = :val_userprofileimage"
			dk.conditionNames().size() == 5  // user, id, attr_user, attr_profile, attr_image
			dk.conditionNames() == [
				'#user': 'user',
				'#id': 'id',
				'#attr_user': 'user',
				'#attr_profile': 'profile', 
				'#attr_image': 'image'
			]
			dk.conditionValues() == [
				':userid': fromS('USER001'),
				':val_userprofileimage': fromB(sdkBytes)
			]
	} // }}}

	// Sort Key Range Condition Tests

	def "Should create KeyFilter with sort key range using string partition key"() { // {{{
		given:
			def sortFilter = greater('timestamp', 1621234567)

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			noExceptionThrown()
		and:
			km.composite() == true
		and:
			km.toMap() == [userId: fromS('user123')]
		and:
			km.sort() == Optional.empty()  // Cannot extract sort key when using range conditions
	} // }}}

	def "Should create KeyFilter with sort key range using numeric partition key"() { // {{{
		given:
			def sortFilter = between('score', 100, 200)

		when:
			KeyFilter km = KeyFilter.of('gameId', 12345, sortFilter)

		then:
			noExceptionThrown()
		and:
			km.composite() == true
		and:
			km.toMap() == [gameId: fromN('12345')]
	} // }}}

	def "Should create KeyFilter with sort key range using binary partition key"() { // {{{
		given:
			def sortFilter = lessOrEqual('created', '2025-01-01')

		when:
			KeyFilter km = KeyFilter.of('binaryKey', sdkBytes, sortFilter)

		then:
			noExceptionThrown()
		and:
			km.composite() == true
		and:
			km.toMap() == [binaryKey: fromB(sdkBytes)]
	} // }}}

	def "Should create KeyFilter with sort key range using byte array partition key"() { // {{{
		given:
			def sortFilter = matchAny('status', 'ACTIVE', 'PENDING')
			byte[] byteArray = 'binary'.getBytes()

		when:
			KeyFilter km = KeyFilter.of('data', byteArray, sortFilter)

		then:
			noExceptionThrown()
		and:
			km.composite() == true
		and:
			km.toMap() == [data: fromB(fromByteArray(byteArray))]
	}

	def "Should generate correct condition expression with sort key range"() {
		given:
			def sortFilter = greater('timestamp', 1621234567)

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition() == "#userId = :userId AND #attr_timestamp > :val_timestamp"
		and:
			km.conditionNames() == [
				'#userId': 'userId',
				'#attr_timestamp': 'timestamp'
			]
		and:
			km.conditionValues() == [
				':userId': fromS('user123'),
				':val_timestamp': fromN('1621234567')
			]
	} // }}}

	def "Should support complex sort key range conditions"() { // {{{
		given:
			def sortFilter = greater('timestamp', 1621234567)
				.and(lessOrEqual('timestamp', 1621334567))

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition().contains("#userId = :userId")
			km.condition().contains("#attr_timestamp > :val_timestamp")
			km.condition().contains("#attr_timestamp <= :val_timestamp_1")
		and:
			km.conditionNames().size() == 2  // userId and timestamp
		and:
			km.conditionValues().size() == 3  // userId value + 2 timestamp values
	} // }}}

	def "Should support BETWEEN range conditions"() { // {{{
		given:
			def sortFilter = between('score', 100, 200)

		when:
			KeyFilter km = KeyFilter.of('gameId', 12345, sortFilter)

		then:
			km.condition() == "#gameId = :gameId AND #attr_score BETWEEN :val_score_start AND :val_score_end"
		and:
			km.conditionNames() == [
				'#gameId': 'gameId',
				'#attr_score': 'score'
			]
		and:
			km.conditionValues() == [
				':gameId': fromN('12345'),
				':val_score_start': fromN('100'),
				':val_score_end': fromN('200')
			]
	} // }}}

	def "Should support IN conditions for sort key"() { // {{{
		given:
			def sortFilter = matchAny('status', 'ACTIVE', 'PENDING', 'PROCESSING')

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition() == "#userId = :userId AND #attr_status IN (:val_status_0, :val_status_1, :val_status_2)"
		and:
			km.conditionNames().size() == 2
		and:
			km.conditionValues().size() == 4  // 1 partition key + 3 status values
	} // }}}

	def "Should support string comparison range conditions"() { // {{{
		given:
			def sortFilter = greaterOrEqual('eventType', 'ORDER')
				.and(less('eventType', 'PAYMENT'))

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition().contains("#userId = :userId")
			km.condition().contains("#attr_eventType >= :val_eventType")
			km.condition().contains("#attr_eventType < :val_eventType_1")
		and:
			km.conditionNames() == [
				'#userId': 'userId',
				'#attr_eventType': 'eventType'
			]
		and:
			km.conditionValues().size() == 3
	} // }}}

	def "Should support begins_with conditions for sort key"() { // {{{
		given:
			def sortFilter = beginsWith('eventType', 'ORDER')

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition() == "#userId = :userId AND begins_with(#attr_eventType, :val_eventType)"
		and:
			km.conditionNames() == [
				'#userId': 'userId',
				'#attr_eventType': 'eventType'
			]
		and:
			km.conditionValues() == [
				':userId': fromS('user123'),
				':val_eventType': fromS('ORDER')
			]
	} // }}}

	def "Should support contains conditions for sort key"() { // {{{
		given:
			def sortFilter = contains('description', 'premium')

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition() == "#userId = :userId AND contains(#attr_description, :val_description)"
		and:
			km.conditionNames() == [
				'#userId': 'userId',
				'#attr_description': 'description'
			]
		and:
			km.conditionValues() == [
				':userId': fromS('user123'),
				':val_description': fromS('premium')
			]
	} // }}}

	def "Should support NOT conditions for sort key"() { // {{{
		given:
			def sortFilter = match('status', 'DELETED').not()

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition() == "#userId = :userId AND NOT #attr_status = :val_status"
		and:
			km.conditionNames() == [
				'#userId': 'userId',
				'#attr_status': 'status'
			]
		and:
			km.conditionValues() == [
				':userId': fromS('user123'),
				':val_status': fromS('DELETED')
			]
	} // }}}

	def "Should support complex boolean combinations"() { // {{{
		given:
			def sortFilter = greater('timestamp', 1621234567)
				.and(match('status', 'ACTIVE'))
				.or(match('priority', 'HIGH'))

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition().contains("#userId = :userId")
			km.condition().contains("AND")
			km.condition().contains("OR")
		and:
			km.conditionNames().size() == 4  // userId, timestamp, status, priority
		and:
			km.conditionValues().size() == 4  // userId + timestamp + status + priority
	} // }}}

	def "Should handle nested field paths in sort key ranges"() { // {{{
		given:
			def sortFilter = greater('user.stats.score', 1000)

		when:
			KeyFilter km = KeyFilter.of('gameId', 12345, sortFilter)

		then:
			km.condition() == "#gameId = :gameId AND #attr_user.#attr_stats.#attr_score > :val_userstatsscore"
		and:
			km.conditionNames() == [
				'#gameId': 'gameId',
				'#attr_user': 'user',
				'#attr_stats': 'stats', 
				'#attr_score': 'score'
			]
		and:
			km.conditionValues() == [
				':gameId': fromN('12345'),
				':val_userstatsscore': fromN('1000')
			]
	} // }}}

	  def "Should handle undefined and defined conditions"() { // {{{		given:
			def sortFilter = defined('deletedAt')

		when:
			KeyFilter km = KeyFilter.of('userId', 'user123', sortFilter)

		then:
			km.condition() == "#userId = :userId AND attribute_exists(#attr_deletedAt) AND NOT #attr_deletedAt = :val_deletedAt"
		and:
			km.conditionNames() == [
				'#userId': 'userId',
				'#attr_deletedAt': 'deletedAt'
			]
		and:
			km.conditionValues().size() == 2
	} // }}}

	def "Should throw exception when creating range filter with multiple partition keys"() { // {{{
		given:
			def sortFilter = greater('timestamp', 1621234567)
			def multiKeyMap = [userId: fromS('user123'), gameId: fromN('456')]

		when:
			new KeyFilter(multiKeyMap, sortFilter)

		then:
			IllegalArgumentException e = thrown()
			e.message == "Key with sort filter must contain only partition key, got size: 2"
	} // }}}

	def "Should maintain backward compatibility - existing behavior unchanged"() { // {{{
		when:
			KeyFilter traditional = KeyFilter.of('userId', 'user123', 'timestamp', 1621234567)
			KeyFilter ranged = KeyFilter.of('userId', 'user123', match('timestamp', 1621234567))

		then:
			// Both should be composite
			traditional.composite() == true
			ranged.composite() == true
		and:
			// Both should allow sort key extraction for simple equality conditions
			traditional.sort().isPresent() == true
			traditional.sort().get().toMap() == [timestamp: fromN('1621234567')]
		and:
			// Ranged with simple match should also allow sort key extraction
			ranged.sort().isPresent() == true
			ranged.sort().get().toMap() == [timestamp: fromN('1621234567')]
		and:
			// Both should generate consistent DynamoFilter-based conditions
			traditional.condition() == "#userId = :userId AND #attr_timestamp = :val_timestamp"
			ranged.condition() == "#userId = :userId AND #attr_timestamp = :val_timestamp"
	} // }}}

}
// vim: fdm=marker
