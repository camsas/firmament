// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// URI tools.

#ifndef FIRMAMENT_MISC_URI_TOOLS_H
#define FIRMAMENT_MISC_URI_TOOLS_H

#include <string>
#include <boost/regex.hpp>

namespace firmament {

#define NETWORKED_REGEX "^(tcp|udp):([a-zA-Z0-9\\.]+):([0-9]+)$"

class URITools {
 public:
  static string GetHostnameFromURI(const string& uri) {
    boost::regex e(NETWORKED_REGEX);
    boost::smatch m;
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
