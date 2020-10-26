#include "cdc/s3_log.hh"
#include "database.hh"

void cdc::s3_log_accumulator::append_cdc_log_mutations(const std::vector<mutation>& cdc_log_mutations) {
    for (const auto& mut : cdc_log_mutations) {
        const ::schema& s = *mut.schema();
        const auto& dk = mut.decorated_key();

        auto table_name = s.ks_name() + "." + s.cf_name();

        auto type_iterator = dk._key.get_compound_type(s)->types().begin();
        auto column_iterator = s.partition_key_columns().begin();

        sstring key_string;

        for (auto&& e : dk._key.components(s)) {
            key_string += (*type_iterator)->to_string(to_bytes(e));
            ++type_iterator;
            ++column_iterator;
        }

        auto& partition = mut.partition();

        auto mut_string = table_name + "," + key_string;

        for (const auto& re : partition.clustered_rows()) {
            sstring row_string = mut_string;

            position_in_partition pip(re.position());
            if (pip.get_clustering_key_prefix()) {
                auto ck = *pip.get_clustering_key_prefix();
                type_iterator = ck.get_compound_type(s)->types().begin();
                column_iterator = s.clustering_key_columns().begin();

                for (auto&& e : ck.components(s)) {
                    row_string += ",";
                    row_string += (*type_iterator)->to_string(to_bytes(e));
                    ++type_iterator;
                    ++column_iterator;
                }
            }

            re.row().cells().for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell_or_collection) {
                auto& column_def = s.column_at(column_kind::regular_column, c_id);
                auto cell = cell_or_collection.as_atomic_cell(column_def);
                auto& type = *column_def.type;

                row_string += ",";
                row_string += type.to_string(cell.value().linearize());
            });

            row_string += "\n";

            _current_chunk += to_bytes(row_string);
        }
    }
}

constexpr int chunk_size = 4096;
constexpr int chunks_per_partition = 100;

std::vector<mutation> cdc::s3_log_accumulator::generate_mutations() {
    if (!_initialized) {
        _initialized = true;

        _object_id = utils::make_random_uuid();
        _blob_id = utils::make_random_uuid();
        _object_name = "cdc_log_" + _object_id.to_sstring() + ".csv";
    }
    
    std::vector<mutation> result;
    if (_current_chunk.size() < chunk_size) {
        return result;
    }

    auto& db = service::get_local_storage_proxy().get_db().local();
   
    auto s3_object_schema = db.find_schema("s3", "object");
    auto s3_version_schema = db.find_schema("s3", "version");
    auto s3_chunk_schema = db.find_schema("s3", "chunk");
    auto s3_part_schema = db.find_schema("s3", "part");
    auto s3_bucket_schema = db.find_schema("s3", "bucket");

    api::timestamp_type timestamp = api::new_timestamp();

    while (_current_chunk.size() >= chunk_size) {
        auto processed_chunk = _current_chunk.substr(0, chunk_size);
        _current_chunk = _current_chunk.substr(chunk_size, _current_chunk.size() - chunk_size);

        auto chunk_pk = partition_key::from_exploded(*s3_chunk_schema, { _blob_id.serialize(), int32_type->decompose(_current_chunk_number / chunks_per_partition) });
        auto ix = clustering_key::from_exploded(*s3_chunk_schema, { int32_type->decompose(_current_chunk_number % chunks_per_partition) });

        mutation chunk_mutation(s3_chunk_schema, chunk_pk);
        chunk_mutation.set_clustered_cell(ix, to_bytes("data"), data_value(processed_chunk), timestamp);
        result.push_back(chunk_mutation);

        _current_chunk_number++;
    }

    utils::UUID bucket_id("cdc0cdc0-cdc0-cdc0-cdc0-cdc0cdc0cdc0");
    time_native_type creation_date;
    creation_date.nanoseconds = timestamp / 1000;

    mutation bucket_mutation(s3_bucket_schema, partition_key::from_single_value(*s3_bucket_schema, to_bytes("cdc-log")));
    bucket_mutation.set_clustered_cell(clustering_key::make_empty(), to_bytes("bucket_id"), data_value(bucket_id), timestamp);
    bucket_mutation.set_clustered_cell(clustering_key::make_empty(), to_bytes("creation_date"), data_value(creation_date), timestamp);

    auto object_key_ck = clustering_key::from_single_value(*s3_object_schema, to_bytes(_object_name));
    sstring object_metadata_string = R"({"content_type": "text/plain", "creation_date": "2020-10-24T11:36:22.000Z", "md5": "not implemented", "size": )";
    object_metadata_string += std::to_string(_current_chunk_number * chunk_size);
    object_metadata_string += R"(, "blob_id": ")";
    object_metadata_string += _blob_id.to_sstring();
    object_metadata_string += R"(", "chunk_size": )";
    object_metadata_string += std::to_string(chunk_size);
    object_metadata_string += R"(, "chunks_per_partition": )";
    object_metadata_string += std::to_string(chunks_per_partition);
    object_metadata_string += R"(})";
    data_value object_metadata(object_metadata_string);

    mutation object_mutation(s3_object_schema, partition_key::from_single_value(*s3_object_schema, bucket_id.serialize()));
    object_mutation.set_clustered_cell(object_key_ck, to_bytes("metadata"), object_metadata, timestamp);
    object_mutation.set_clustered_cell(object_key_ck, to_bytes("object_id"), data_value(_object_id), timestamp);
    object_mutation.set_clustered_cell(object_key_ck, to_bytes("version"), data_value(1), timestamp);

    auto object_key_pk = partition_key::from_single_value(*s3_version_schema, _object_id.serialize());
    auto version_ck = clustering_key::from_exploded(*s3_version_schema, { int32_type->decompose(1) });

    mutation version_mutation(s3_version_schema, object_key_pk);
    version_mutation.set_clustered_cell(version_ck, to_bytes("bucket_id"), data_value(bucket_id), timestamp);
    version_mutation.set_clustered_cell(version_ck, to_bytes("chunk_size"), data_value(chunk_size), timestamp);
    version_mutation.set_clustered_cell(version_ck, to_bytes("chunks_per_partition"), data_value(chunks_per_partition), timestamp);
    version_mutation.set_clustered_cell(version_ck, to_bytes("content_type"), data_value(sstring("text/plain")), timestamp);
    version_mutation.set_clustered_cell(version_ck, to_bytes("creation_date"), data_value(creation_date), timestamp);
    version_mutation.set_clustered_cell(version_ck, to_bytes("md5"), data_value(sstring("not implemented")), timestamp);
    version_mutation.set_clustered_cell(version_ck, to_bytes("size"), data_value(_current_chunk_number * chunk_size), timestamp);  
    version_mutation.set_clustered_cell(version_ck, to_bytes("metadata"), object_metadata, timestamp);  
    
    auto object_key_pk2 = partition_key::from_single_value(*s3_part_schema, _object_id.serialize());
    auto version_part_ck = clustering_key::from_exploded(*s3_part_schema, { int32_type->decompose(1), int32_type->decompose(1) });

    mutation part_mutation(s3_part_schema, object_key_pk2);
    part_mutation.set_clustered_cell(version_part_ck, to_bytes("blob_id"), data_value(_blob_id), timestamp);
    part_mutation.set_clustered_cell(version_part_ck, to_bytes("md5"), data_value(sstring("not implemented")), timestamp);    
    part_mutation.set_clustered_cell(version_part_ck, to_bytes("size"), data_value(_current_chunk_number * chunk_size), timestamp);  

    result.push_back(bucket_mutation);
    result.push_back(object_mutation);
    result.push_back(version_mutation);
    result.push_back(part_mutation);

    return result;
}
