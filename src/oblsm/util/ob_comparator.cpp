/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/util/ob_comparator.h"
#include "oblsm/ob_lsm_define.h"
#include "oblsm/util/ob_coding.h"

namespace oceanbase {
int ObDefaultComparator::compare(const string_view &a, const string_view &b) const { return a.compare(b); }

int ObInternalKeyComparator::compare(const string_view &a, const string_view &b) const
{
  const string_view akey = extract_user_key(a);
  const string_view bkey = extract_user_key(b);
  int               r    = default_comparator_.compare(akey, bkey);
  if (r == 0) {
    // Ensure that 'a' and 'b' are actual internal keys with enough size for a sequence number.
    // This check might be needed depending on how SEQ_SIZE and extract_user_key are defined.
    // For robustness, ensure a.size() >= SEQ_SIZE and b.size() >= SEQ_SIZE,
    // or more accurately, a.size() > akey.size() if extract_user_key guarantees non-empty suffix.

    // Correctly get the packed sequence number (and type) from the end of the full internal key.
    // The pointer should be (a.data() + a.size() - SEQ_SIZE).
    // get_numeric should read SEQ_SIZE bytes from this location.
    // This assumes SEQ_SIZE is the size of the packed sequence number + type.
    uint64_t a_packed_seq_type = 0;
    uint64_t b_packed_seq_type = 0;

    // It's safer to calculate the offset from the start of the user key within the internal key,
    // or from the end of the internal key if extract_user_key is simply a prefix.
    // Assuming SEQ_SIZE is defined in ob_lsm_define.h and represents the length of the seq+type suffix.
    // And extract_user_key(a) returns a view of a.data() with length a.size() - SEQ_SIZE.
    // Then the sequence number starts at a.data() + akey.size() or a.data() + (a.size() - SEQ_SIZE).
    // Let's use a.data() + (a.size() - SEQ_SIZE) as it's less dependent on extract_user_key's exact internal split point.
    if (a.size() >= SEQ_SIZE && b.size() >= SEQ_SIZE) { // Basic sanity check
        a_packed_seq_type = get_numeric<uint64_t>(a.data() + (a.size() - SEQ_SIZE));
        b_packed_seq_type = get_numeric<uint64_t>(b.data() + (b.size() - SEQ_SIZE));
    } else {
        // This case should ideally not happen if valid internal keys are passed.
        // Or, if keys can sometimes be user keys, this comparator should handle it or not be used.
        // For now, if sizes are too small, treat sequences as equal or handle error.
        // Keeping r = 0 (meaning only user keys were equal, seq considered equal or indeterminable).
        return r; // Or log an error
    }


    // Extract the actual sequence number. This depends on how sequence and type are packed.
    // Example: if type is 1 byte (LSB) and sequence is 7 bytes (MSB) within the 8-byte SEQ_SIZE.
    uint64_t aseq = a_packed_seq_type >> 8; // Example: Shift out 1 byte for type
    uint64_t bseq = b_packed_seq_type >> 8; // Example: Shift out 1 byte for type

    if (aseq > bseq) { // Larger sequence number means "older" or "smaller" in sort order
      r = -1;
    } else if (aseq < bseq) {
      r = +1;
    }
    // If r is still 0, means user keys and sequence numbers are identical.
    // Could further compare by type if necessary (e.g. kTypeValue vs kTypeDeletion).
    // Current logic doesn't distinguish by type if user key and seq are same.
  }
  return r;
}

}  // namespace oceanbase