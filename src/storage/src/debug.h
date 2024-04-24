//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_DEBUG_H_
#define SRC_DEBUG_H_


#  define TRACE(M, ...) {}
#  define DEBUG(M, ...) {}

static std::string get_printable_key(const std::string& key) {
  std::string res;
  for (int i = 0; i < key.size(); i++) {
    if (std::isprint(key[i])) {
      res.append(1, key[i]);
    } else {
      char tmp[3];
      snprintf(tmp, 2, "%02x", key[i] & 0xFF);
      res.append(tmp, 2);
    }
  }
  return res;
}


#endif  // SRC_DEBUG_H_
