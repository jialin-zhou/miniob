/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
   miniob is licensed under Mulan PSL v2.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details. */

#include "oblsm/wal/ob_lsm_wal.h"
#include "common/log/log.h"
#include "oblsm/util/ob_file_reader.h"
#include "oblsm/util/ob_file_writer.h"
#include "common/lang/filesystem.h"
#include <cstdio>
#include <filesystem>

namespace oceanbase {

// NOTE: These encoding/decoding functions are assumed to exist in your codebase.
// If not, you'll need to add them. They are standard for serialization.
// You can place them in a utility header.
inline void encode_fixed64(char* buf, uint64_t value) {
    memcpy(buf, &value, sizeof(value));
}

inline uint64_t decode_fixed64(const char* ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

/**
 * @brief Opens the WAL file for writing.
 */
RC WAL::open(const std::string &filename)
{
    if (writer_ && writer_->is_open()) {
        // Already open, maybe log a warning or return success.
        return RC::SUCCESS;
    }
    filename_ = filename;
    writer_ = ObFileWriter::create_file_writer(filename, true); // true for append mode
    RC rc = writer_->open_file();
    if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to open WAL writer for file: %s", filename.c_str());
        writer_ = nullptr;
        return RC::IOERR_OPEN;
    }
    return RC::SUCCESS;
}

/**
 * @brief Synchronizes the WAL to disk.
 */
RC WAL::sync()
{
    if (!writer_) {
        LOG_WARN("WAL writer is not initialized, cannot sync.");
        return RC::FILE_NOT_EXIST;
    }
    return writer_->flush(); // The sync operation is called 'flush' in ObFileWriter
}

/**
 * @brief Writes a key-value pair to the WAL.
 *
 * This function serializes the data according to the format:
 * [seq (8 bytes)][key_len (8 bytes)][key_data][val_len (8 bytes)][val_data]
 */
RC WAL::put(uint64_t seq, string_view key, string_view val)
{
    // printf("put seq=%lu, key=%s, val=%s\n", seq, key.data(), val.data());
    // printf("filename=%s\n", filename_.c_str());
    open(filename_);
    if (!writer_ || !writer_->is_open()) {
        LOG_ERROR("WAL writer is not initialized or file is not open. Did you call open()?");
        return RC::FILE_NOT_EXIST;
    }

    size_t key_len = key.size();
    size_t val_len = val.size();

    // The header contains seq, key_len, and val_len.
    const size_t header_size = sizeof(uint64_t) * 3; // Use sizeof(uint64_t) for lengths for consistency
    char header_buf[header_size];

    // 1. Serialize sequence number
    encode_fixed64(header_buf, seq);
    // 2. Serialize key length
    encode_fixed64(header_buf + sizeof(uint64_t), key_len);
    // 3. Serialize value length
    encode_fixed64(header_buf + sizeof(uint64_t) * 2, val_len);

    // Combine header and data into a single buffer for one write call.
    string record_data;
    record_data.reserve(header_size + key_len + val_len);
    record_data.append(header_buf, header_size);
    record_data.append(key);
    record_data.append(val);

    // Use the 'write' method from ObFileWriter
    RC rc = writer_->write(record_data);
    
    if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to write to WAL file %s, rc=%d", filename_.c_str(), rc);
    }

    return rc;
}


/**
 * @brief Recovers data from a specified WAL file.
 */
RC WAL::recover(const std::string &wal_file, std::vector<WalRecord> &wal_records)
{
    if (!std::filesystem::exists(wal_file)) {
        LOG_INFO("WAL file %s not found, skipping recovery.", wal_file.c_str());
        return RC::SUCCESS;
    }
    
    auto reader = ObFileReader::create_file_reader(wal_file);
    if (nullptr == reader) {
        LOG_ERROR("Failed to create file reader for WAL %s", wal_file.c_str());
        return RC::IOERR_OPEN;
    }
    RC rc = reader->open_file();
    if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to open file reader for WAL %s", wal_file.c_str());
        return RC::IOERR_OPEN;
    }

    wal_records.clear();
    const uint32_t total_size = reader->file_size();
    uint32_t current_pos = 0;

    const size_t header_size = sizeof(uint64_t) * 3;

    while (current_pos + header_size <= total_size) {
        // 1. Read the fixed-size header first.
        string header_data = reader->read_pos(current_pos, header_size);
        if (header_data.size() != header_size) {
            LOG_ERROR("Failed to read WAL header from %s at pos %u. File may be corrupt.", wal_file.c_str(), current_pos);
            return RC::IOERR_READ; // Corrupted file
        }
        current_pos += header_size;

        // 2. Decode header
        const char* p = header_data.data();
        uint64_t seq = decode_fixed64(p);
        size_t key_len = decode_fixed64(p + sizeof(uint64_t));
        size_t val_len = decode_fixed64(p + sizeof(uint64_t) * 2);
        
        if (current_pos + key_len + val_len > total_size) {
            LOG_ERROR("Incomplete WAL record in %s. File may be corrupt.", wal_file.c_str());
            return RC::IOERR_READ; // Corrupted file
        }

        // 3. Read key and value
        string key = reader->read_pos(current_pos, key_len);
        current_pos += key_len;

        string val = reader->read_pos(current_pos, val_len);
        current_pos += val_len;

        if (key.size() != key_len || val.size() != val_len) {
            LOG_ERROR("Failed to read key/value data from %s. File may be corrupt.", wal_file.c_str());
            return RC::IOERR_READ;
        }
        
        wal_records.emplace_back(seq, std::move(key), std::move(val));
    }
    
    LOG_INFO("WAL recovery complete for %s, loaded %zu records.", wal_file.c_str(), wal_records.size());
    return RC::SUCCESS;
}
}  // namespace oceanbase