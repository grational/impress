package it.grational.storage.dynamodb

import groovy.transform.CompileStatic

/**
 * Utility class for handling nested attribute paths in DynamoDB expressions.
 * Provides methods for processing nested field paths in DynamoDB expressions,
 * generating proper placeholders, and maintaining attribute mappings.
 */
@CompileStatic
class NestedPathProcessor {
	/**
	 * Prefix used for attribute name placeholders in expression attribute names for DynamoFilter
	 */
	static final String FILTER_ATTRIBUTE_PREFIX = "#attr_"

	/**
	 * Prefix used for attribute name placeholders in expression attribute names for DynamoKey
	 */
	static final String KEY_ATTRIBUTE_PREFIX = "#"

	/**
	 * Prefix used for attribute value placeholders in expression attribute values
	 */
	static final String VALUE_PREFIX = ":val_"

	/**
	 * Result class for processNestedPath operations
	 */
	static class PathResult {
		String nameRef
		Map<String, String> nameMap
	}

	/**
	 * Processes a possibly nested field path using dot notation for DynamoFilter
	 * and returns the proper expression attribute name reference
	 * along with the attribute name mappings
	 *
	 * @param path The field path, possibly with dot notation (e.g. "user.address.zipcode")
	 * @return A PathResult with nameRef and nameMap properties
	 */
	static PathResult processForFilter(String path) {
		return processNestedPath(path, FILTER_ATTRIBUTE_PREFIX)
	}

	/**
	 * Processes a possibly nested field path using dot notation for DynamoKey
	 * and returns the proper expression attribute name reference
	 * along with the attribute name mappings
	 *
	 * @param path The field path, possibly with dot notation (e.g. "user.address.zipcode")
	 * @return A PathResult with nameRef and nameMap properties
	 */
	static PathResult processForKey(String path) {
		return processNestedPath(path, KEY_ATTRIBUTE_PREFIX)
	}

	/**
	 * Core implementation of nested path processing
	 */
	private static PathResult processNestedPath(String path, String prefix) {
		PathResult result = new PathResult()

		if (path == null) {
			result.nameRef = ""
			result.nameMap = [:]
			return result
		}

		if (!path.contains('.')) {
			// Simple case: no nested fields
			String safe = sanitize(path)
			String nameRef = "${prefix}${safe}"
			result.nameRef = nameRef
			result.nameMap = [(nameRef): path]
			return result
		}

		// Handle nested field path
		String[] parts = path.split(/\./)
		Map<String, String> nameMap = [:]
		List<String> nameRefs = []

		parts.each { String part ->
			String safe = sanitize(part)
			String nameRef = "${prefix}${safe}"
			nameMap[nameRef] = part
			nameRefs << nameRef
		}

		result.nameRef = nameRefs.join('.')
		result.nameMap = nameMap
		return result
	}

	/**
	 * Sanitizes field names by removing special characters
	 * @param name The attribute name to sanitize
	 * @return A sanitized version of the name suitable for use in expression placeholders
	 */
	static String sanitize(String name) {
		return name?.replaceAll(/[^a-zA-Z0-9_]/,'') ?: ''
	}

	/**
	 * Generates a safe placeholder value name for the attribute
	 * that removes dots and other special characters
	 * @param path The attribute path
	 * @return A sanitized string suitable for use in value placeholders
	 */
	static String safeValueName(String path) {
		return path?.replaceAll(/[^a-zA-Z0-9_]/, '') ?: ''
	}

	/**
	 * Creates a value placeholder for a given attribute path
	 * @param path The attribute path
	 * @return A properly formatted value placeholder
	 */
	static String createValuePlaceholder(String path) {
		return "${VALUE_PREFIX}${safeValueName(path)}"
	}
}
