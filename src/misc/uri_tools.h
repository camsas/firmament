/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

// URI tools.

#ifndef FIRMAMENT_MISC_URI_TOOLS_H
#define FIRMAMENT_MISC_URI_TOOLS_H

#include <string>
#include <boost/regex.hpp>

namespace firmament {

#define NETWORKED_REGEX "^(tcp|udp):([a-zA-Z0-9\\.\\-]+):([0-9]+)$"

class URITools {
 public:
  static string GetHostnameFromURI(const string& uri) {
    boost::regex e(NETWORKED_REGEX);
    boost::smatch m;
    CHECK(!uri.empty()) << "Empty URI passed to GetHostnameFromURI";
    // Try to match the appropriate regular expression, and find the hostname
    // component.
    if (boost::regex_match(uri, m, e, boost::match_extra)
        && m.size() == 4) {
      VLOG(3) << "Parsed hostname: " << m[2];
      return m[2];  // hostname component of match
    } else {
      LOG(WARNING) << "No hostname could be matched in URI " << uri << "! ("
                   << m.size() << " matches).";
      return "";
    }
  }

  static string GetPortFromURI(const string& uri) {
    boost::regex e(NETWORKED_REGEX);
    boost::smatch m;
    CHECK(!uri.empty()) << "Empty URI passed to GetPortFromURI";
    // Magic
    if (boost::regex_match(uri, m, e, boost::match_extra)
        && m.size() == 4) {
      VLOG(3) << "Parsed port: " << m[3];
      return m[3];  // port number component of match
    } else {
      LOG(WARNING) << "No port could be matched in URI " << uri << "!";
      return "";
    }
  }
};


}  // namespace firmament

#endif  // FIRMAMENT_MISC_URI_TOOLS_H
