#include "duckdb.hpp"
#include "iceberg_utils.hpp"
#include "zlib.h"
#include "fstream"

namespace duckdb {

string IcebergUtils::FileToString(const string &path, FileSystem &fs) {
	auto handle =
	    fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK, FileSystem::DEFAULT_COMPRESSION);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	printf("content=%s\n", ret_val.c_str());
	return ret_val;
}

// Function to decompress a gz file content string
string IcebergUtils::GzFileToString(const string &path, FileSystem &fs) {
  // Initialize zlib variables
  string gzipped_string = FileToString(path, fs);
  std::stringstream decompressed;
    int CHUNK_SIZE = 16384;
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = gzipped_string.size();
    zs.next_in = (Bytef *)gzipped_string.data();
    int ret = inflateInit2(&zs, 16 + MAX_WBITS); // MAX_WBITS + 16 to enable gzip decoding
    if (ret != Z_OK)
    {
        throw std::runtime_error("inflateInit failed");
    }
    do
    {
        char out[CHUNK_SIZE];
        zs.avail_out = CHUNK_SIZE;
        zs.next_out = (Bytef *)out;
        ret = inflate(&zs, Z_NO_FLUSH);
        if (ret < 0)
        {
            inflateEnd(&zs);
            throw std::runtime_error("inflate failed with error code " + to_string(ret));
        }
        decompressed.write(out, CHUNK_SIZE - zs.avail_out);
    } while (zs.avail_out == 0);
    inflateEnd(&zs);
    string ds = decompressed.str();
    printf("content=%s\n", ds.c_str());
    return ds;
}

string IcebergUtils::GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {
	std::size_t found = relative_file_path.find("/metadata/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	found = relative_file_path.find("/data/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	throw IOException("Did not recognize iceberg path");
}

uint64_t IcebergUtils::TryGetNumFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_NUM) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_uint(val);
}

bool IcebergUtils::TryGetBoolFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	// uint8_t tag = yyjson_get_tag(val);
	printf("TryGetBoolFromObject: field=%s; tag=%d; value=%s;\n", field.c_str(), yyjson_get_tag(val), yyjson_get_str(val));
	// if (yyjson_get_tag(val) == YYJSON_TYPE_BOOL) {
	// 	return yyjson_get_bool(val);
	// }
	// if (yyjson_get_tag(val) == YYJSON_TYPE_STR) {
	// 	return strcmp(yyjson_get_str(val), "true") == 0;
	// }

	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		printf("fields=%s; null!\n", field.c_str());
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		printf("fields=%s; string!\n", field.c_str());
		return strcmp(yyjson_get_str(val), "true") == 0;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
		printf("fields=%s; array!\n", field.c_str());
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		printf("fields=%s; object!\n", field.c_str());
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		printf("fields=%s; boolean!\n", field.c_str());
		return yyjson_get_bool(val);
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		printf("fields=%s; unsigned integer!\n", field.c_str());
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		printf("fields=%s; bigint!\n", field.c_str());
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		printf("fields=%s; unsigned double!\n", field.c_str());
	default:
		throw InternalException("Unexpected yyjson tag!");
	}

	printf("yyjson_get_type_desc(val): %s\n", yyjson_get_type_desc(val));
	throw IOException("Invalid field, unwanted datatype found while parsing field: " + field);
}

string IcebergUtils::TryGetStrFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_STR) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_str(val);
}

string IcebergUtils::TryGetStrRecursiveFromObject(yyjson_val *obj, const string &field) {
	string nameKey = "name";
	auto name = yyjson_obj_getn(obj, nameKey.c_str(), nameKey.size());
    printf("1.TryGetStrRecursiveFromObject enter! objname=%s; field=%s;\n", yyjson_get_str(name), field.c_str());
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val) {
        printf("2.TryGetStrRecursiveFromObject: Found empty value for field: %s\n", field.c_str());
		throw IOException("TryGetStrRecursiveFromObject: Invalid field found while parsing field: " + field);
	}
    if (yyjson_get_tag(val) == YYJSON_TYPE_STR) {
        printf("3.TryGetStrRecursiveFromObject: obj=%s; field=%s; tag=%llu; uni=%c;\n",  yyjson_get_str(name), field.c_str(), val->tag, val->uni);
        printf("4.TryGetStrRecursiveFromObject final exit!\n");
    	return yyjson_get_str(val);
    }
	printf("5.TryGetStrRecursiveFromObject: obj=%s; field=%s;\n", yyjson_get_str(name), field.c_str());
    return TryGetStrRecursiveFromObject(val, field);
}

} // namespace duckdb