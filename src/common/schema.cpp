#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"

namespace duckdb {

// https://iceberg.apache.org/spec/#schemas

// forward declaration
static LogicalType ParseType(yyjson_val *type);

static LogicalType mapPrimitiveType(string type_str) {
	printf("mapPrimitiveType: %s\n", type_str.c_str());
	if (type_str == "boolean") {
		return LogicalType::BOOLEAN;
	}
	if (type_str == "int") {
		return LogicalType::INTEGER;
	}
	if (type_str == "long") {
		return LogicalType::BIGINT;
	}
	if (type_str == "float") {
		return LogicalType::FLOAT;
	}
	if (type_str == "double") {
		return LogicalType::DOUBLE;
	}
	if (type_str == "date") {
		return LogicalType::DATE;
	}
	if (type_str == "time") {
		return LogicalType::TIME;
	}
	if (type_str == "timestamp") {
		return LogicalType::TIMESTAMP;
	}
	if (type_str == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (type_str == "string") {
		return LogicalType::VARCHAR;
	}
	if (type_str == "uuid") {
		return LogicalType::UUID;
	}
	if (StringUtil::StartsWith(type_str, "fixed")) {
		// FIXME: use fixed size type in DuckDB
		return LogicalType::BLOB;
	}
	if (type_str == "binary") {
		return LogicalType::BLOB;
	}
	if (StringUtil::StartsWith(type_str, "decimal")) {
		D_ASSERT(type_str[7] == '(');
		D_ASSERT(type_str.back() == ')');
		auto start = type_str.find('(');
		auto end = type_str.rfind(')');
		auto raw_digits = type_str.substr(start + 1, end - start);
		auto digits = StringUtil::Split(raw_digits, ',');
		D_ASSERT(digits.size() == 2);

		auto width = std::stoi(digits[0]);
		auto scale = std::stoi(digits[1]);
		return LogicalType::DECIMAL(width, scale);
	}
	throw IOException("Encountered an unrecognized type in JSON schema: \"%s\"", type_str);
}

static LogicalType ParseStruct(yyjson_val *struct_type) {
	D_ASSERT(yyjson_get_tag(struct_type) == YYJSON_TYPE_OBJ);
	D_ASSERT(IcebergUtils::TryGetStrFromObject(struct_type, "type") == "struct");

	child_list_t<LogicalType> children;
	yyjson_val *field;
	size_t max, idx;

	auto fields = yyjson_obj_get(struct_type, "fields");
	yyjson_arr_foreach(fields, idx, max, field) {
		// NOTE: 'id', 'required', 'doc', 'initial_default', 'write_default' are ignored for now
		auto name = IcebergUtils::TryGetStrFromObject(field, "name");
		auto sType = IcebergUtils::TryGetStrFromObject(field, "type");
		auto type = mapPrimitiveType(sType);
		// printf("***name=%s; type=%s;\n", name.c_str(), t.c_str());
		// auto type_item = yyjson_obj_get(field, "type");
		// auto type = ParseType(type_item);
		children.push_back(std::make_pair(name, type));
	}
	return LogicalType::STRUCT(std::move(children));
}

static LogicalType ParseList(yyjson_val *list_type) {
	D_ASSERT(yyjson_get_tag(list_type) == YYJSON_TYPE_OBJ);
	D_ASSERT(IcebergUtils::TryGetStrFromObject(list_type, "type") == "list");

	// NOTE: 'element-id', 'element-required' are ignored for now
	auto element = yyjson_obj_get(list_type, "element");
	auto child_type = ParseType(element);
	return LogicalType::LIST(child_type);
}

static LogicalType ParseMap(yyjson_val *map_type) {
	D_ASSERT(yyjson_get_tag(map_type) == YYJSON_TYPE_OBJ);
	D_ASSERT(IcebergUtils::TryGetStrFromObject(map_type, "type") == "map");

	// NOTE: 'key-id', 'value-id', 'value-required' are ignored for now
	auto key = yyjson_obj_get(map_type, "key");
	auto value = yyjson_obj_get(map_type, "value");

	// auto key_type = ParseType(key);
	// auto value_type = ParseType(value);
	auto key_type = mapPrimitiveType(yyjson_get_str(key));
	auto value_type = mapPrimitiveType(yyjson_get_str(value));
	return LogicalType::MAP(key_type, value_type);
}

static LogicalType ParseComplexType(yyjson_val *type) {
	D_ASSERT(yyjson_get_tag(type) == YYJSON_TYPE_OBJ);
	auto type_str = IcebergUtils::TryGetStrFromObject(type, "type");

	if (type_str == "struct") {
		return ParseStruct(type);
	}
	if (type_str == "list") {
		return ParseList(type);
	}
	if (type_str == "map") {
		return ParseMap(type);
	}
	throw IOException("Invalid field found while parsing field: type");
}

static LogicalType ParseType(yyjson_val *type) {
	auto type_str = IcebergUtils::TryGetStrRecursiveFromObject(type, "type");
	printf("-----\nstart:ParseType=%s\n", type_str.c_str());
	auto val = yyjson_obj_get(type, "type");
	if (!val) {
		throw IOException("Invalid field found while parsing field: type");
	}
	if (yyjson_get_tag(val) == YYJSON_TYPE_OBJ) {
		return LogicalType::VARCHAR;
		// return ParseComplexType(val);
	}
	if (yyjson_get_tag(val) != YYJSON_TYPE_STR) {
		throw IOException("Invalid field found while parsing field: type");
	}
	printf("-----\nend:ParseType=%s\n", type_str.c_str());
	return mapPrimitiveType(type_str);
}

IcebergColumnDefinition IcebergColumnDefinition::ParseFromJson(yyjson_val *val) {
	
	IcebergColumnDefinition ret;

	ret.id = IcebergUtils::TryGetNumFromObject(val, "id");
	ret.name = IcebergUtils::TryGetStrFromObject(val, "name");
	printf("----------------------------------------------------------------------\n");
	printf("ParseFromJson start field=%s!\n", ret.name.c_str());
	ret.type = ParseType(val);
	ret.default_value = Value();
	printf("ParseFromJson: name=%s; val=%s\n", ret.name.c_str(), yyjson_get_str(val));
	string t = "type";
	auto typeVal = yyjson_obj_getn(val, t.c_str(), t.size());
	if (!typeVal) {
		printf("ParseFromJson: type value is empty\n");
	}
	
	auto type_str = IcebergUtils::TryGetStrRecursiveFromObject(val, "type");
	printf("ParseFromJson: name=%s; type_str=%s; val=%s\n", ret.name.c_str(), type_str.c_str(), yyjson_get_str(val));
	
	if (type_str == "map" || type_str == "struct" || type_str == "list") {
		// it's a complex data type
		ret.required = false;
	} else {
		ret.required = IcebergUtils::TryGetBoolFromObject(val, "required");
	}
	printf("ParseFromJson end field=%s!\n", ret.name.c_str());
	printf("----------------------------------------------------------------------\n");
	return ret;
}

static vector<IcebergColumnDefinition> ParseSchemaFromJson(yyjson_val *schema_json) {
	// Assert that the top level 'type' is a struct
	auto type_str = IcebergUtils::TryGetStrFromObject(schema_json, "type");
	if (type_str != "struct") {
		throw IOException("Schema in JSON Metadata is invalid");
	}
	D_ASSERT(yyjson_get_tag(schema_json) == YYJSON_TYPE_OBJ);
	D_ASSERT(IcebergUtils::TryGetStrFromObject(schema_json, "type") == "struct");
	yyjson_val *field;
	size_t max, idx;
	vector<IcebergColumnDefinition> ret;

	auto fields = yyjson_obj_get(schema_json, "fields");
	yyjson_arr_foreach(fields, idx, max, field) {
		ret.push_back(IcebergColumnDefinition::ParseFromJson(field));
	}

	return ret;
}

vector<IcebergColumnDefinition> IcebergSnapshot::ParseSchema(vector<yyjson_val *> &schemas, idx_t schema_id) {
	// Multiple schemas can be present in the json metadata 'schemas' list
	for (const auto &schema_ptr : schemas) {
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema_ptr, "schema-id");
		if (found_schema_id == schema_id) {
			return ParseSchemaFromJson(schema_ptr);
		}
	}

	throw IOException("Iceberg schema with schema id " + to_string(schema_id) + " was not found!");
}

} // namespace duckdb
