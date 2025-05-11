package it.grational.storage.dynamodb

import spock.lang.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.*
import software.amazon.awssdk.core.SdkBytes
import static software.amazon.awssdk.core.SdkBytes.*

class KeyMatchUSpec extends Specification {

	@Shared
	SdkBytes sdkBytes

	def setup() {
		sdkBytes = fromUtf8String('binary')
	}

	def "Should return a valid dynamo key match given each supported type"() {
		when:
			KeyMatch dk = new KeyMatch(key, value)

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
	}

	def "Should return a valid dynamo key also when a combined key is used"() {
		when:
			KeyMatch dk = new KeyMatch(pk, pv, sk, sv)

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
	}

	def "Should return a valid dynamo key with binary data in various composite key combinations"() {
		when:
			KeyMatch dk = new KeyMatch(pk, pv, sk, sv)

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
	}

	def "Should be able to return the key condition expression and the relative placeholders"() {
		when:
			KeyMatch dk = new KeyMatch('string', 'value')

		then:
			dk.composite() == false

		and:
			dk.condition() == "#string = :string"
		and:
			dk.conditionNames() == [ '#string': 'string' ]
		and:
			dk.conditionValues() == [':string': fromS('value')]
	}

	def "Should be able to return the key condition expressions for composite keys"() {
		when:
			KeyMatch dk = new KeyMatch (
				'string', 'value',
				'number', 1
			)

		then:
			dk.composite() == true

		and:
			dk.condition() == "#string = :string AND #number = :number"
		and:
			dk.conditionNames() == [
				'#string': 'string',
				'#number': 'number'
			]
		and:
			dk.conditionValues() == [
				':string': fromS('value'),
				':number': fromN('1'),
			]
	}

	def "Should throw an exception if the key map is invalid"() {
		when:
			new KeyMatch(input)

		then:
			Exception e = thrown(IllegalArgumentException)
		and:
			e.message == exceptionMessage

		where:
			input                                             || exceptionMessage
			[:]                                               || 'Invalid key size: 0'
			[ a: fromS('a'), b: fromS('b'), one: fromN('1') ] || 'Invalid key size: 3'
			[ a: fromBool(true) ]                             || "Unsupported key types: [a:AttributeValue(BOOL=true)]"
	}

	def "Should be able to instantiate it directly with an appropriate map"() {
		when:
			KeyMatch dk = new KeyMatch(input)

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
	}

	def "Should be able to extract a partition key from a composite one"() {
		when:
			KeyMatch dk = new KeyMatch(input)

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
	}

	def "Should be able to extract a sort key from a composite one"() {
		when:
			KeyMatch dk = new KeyMatch(input)

		then:
			dk.sort() == Optional.ofNullable(expected)

		where:
			input                                || expected
			[ string: fromS('value') ]           || null
			[ binary: fromB(sdkBytes) ]          || null
			[ a: fromS('a'), b: fromS('b') ]     || new KeyMatch(b: fromS('b'))
			[ a: fromS('a'), one: fromN('1') ]   || new KeyMatch(one: fromN('1'))
			[ one: fromN('1'), b: fromS('b') ]   || new KeyMatch(b: fromS('b'))
			[ one: fromN('1'), two: fromN('2') ] || new KeyMatch(two: fromN('1'))
			[ a: fromS('a'), bin: fromB(sdkBytes) ] || new KeyMatch(bin: fromB(sdkBytes))
			[ bin: fromB(sdkBytes), a: fromS('a') ] || new KeyMatch(a: fromS('a'))
			[ bin1: fromB(sdkBytes), bin2: fromB(sdkBytes) ] || new KeyMatch(bin2: fromB(sdkBytes))
	}

	def "Should handle nested field paths in condition expression"() {
		when:
			KeyMatch dk = new KeyMatch('user.profile.id', 'ABC123')

		then:
			dk.condition() == "#user.#profile.#id = :userprofileid"
			dk.conditionNames() == [
				'#user': 'user',
				'#profile': 'profile',
				'#id': 'id'
			]
			dk.conditionValues() == [':userprofileid': fromS('ABC123')]
	}

	def "Should handle nested field paths in composite keys"() {
		when:
			KeyMatch dk = new KeyMatch(
				'user.id', 'USER001',
				'user.group.name', 'ADMIN'
			)

		then:
			dk.condition() == "#user.#id = :userid AND #user.#group.#name = :usergroupname"
			dk.conditionNames().size() == 4  // user, id, group, name
			dk.conditionNames() == [
				'#user': 'user',
				'#id': 'id',
				'#group': 'group',
				'#name': 'name'
			]
			dk.conditionValues() == [
				':userid': fromS('USER001'),
				':usergroupname': fromS('ADMIN')
			]
	}

	def "Should handle deeply nested field paths"() {
		when:
			KeyMatch dk = new KeyMatch('data.user.profile.contact.email', 'test@example.com')

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
	}
	
	def "Should handle special characters in path segments"() {
		when:
			KeyMatch dk = new KeyMatch('user-data.custom_field', 'value')

		then:
			dk.condition() == "#userdata.#custom_field = :userdatacustom_field"
			dk.conditionNames() == [
				'#userdata': 'user-data',
				'#custom_field': 'custom_field'
			]
			dk.conditionValues() == [':userdatacustom_field': fromS('value')]
	}

	def "Should handle binary data in condition expressions"() {
		when:
			KeyMatch dk = new KeyMatch('binary_data', sdkBytes)

		then:
			dk.condition() == "#binary_data = :binary_data"
			dk.conditionNames() == [
				'#binary_data': 'binary_data'
			]
			dk.conditionValues() == [':binary_data': fromB(sdkBytes)]
	}

	def "Should handle binary data in nested field paths"() {
		when:
			KeyMatch dk = new KeyMatch('user.profile.image', sdkBytes)

		then:
			dk.condition() == "#user.#profile.#image = :userprofileimage"
			dk.conditionNames() == [
				'#user': 'user',
				'#profile': 'profile',
				'#image': 'image'
			]
			dk.conditionValues() == [':userprofileimage': fromB(sdkBytes)]
	}

	def "Should handle binary data in composite keys with nested paths"() {
		when:
			KeyMatch dk = new KeyMatch (
				'user.id', 'USER001',
				'user.profile.image',
				sdkBytes
			)

		then:
			dk.condition() == "#user.#id = :userid AND #user.#profile.#image = :userprofileimage"
			dk.conditionNames().size() == 4  // user, id, profile, image
			dk.conditionNames() == [
				'#user': 'user',
				'#id': 'id',
				'#profile': 'profile',
				'#image': 'image'
			]
			dk.conditionValues() == [
				':userid': fromS('USER001'),
				':userprofileimage': fromB(sdkBytes)
			]
	}
}
