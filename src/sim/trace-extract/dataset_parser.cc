#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

#include "dataset_parser.h"

namespace fs = boost::filesystem;

namespace firmament {
namespace sim {

DatasetParser::DatasetParser(std::string trace_path,
		                           std::string dataset_name)
				: dataset_path(trace_path), current_index(0) {
	dataset_path /= fs::path(dataset_name);
	fs::directory_iterator end_it;
	unsigned int num_files_seen = 0, index, num_files;
	for (fs::directory_iterator it(dataset_path); it != end_it; ++it) {
		std::string fname = it->path().filename().string();
		int num_matches = sscanf(fname.c_str(), "part-%5u-of-%5u.csv",
				 	 	 	 	 &index, &num_files);
		CHECK_EQ(num_matches, 2) << "unrecognized file format " << fname;

		if (num_files_seen == 0) {
			// first filename we have read
			this->num_files = num_files;
		}
		CHECK_EQ(this->num_files, num_files)
			<< "corrupt cluster trace? disagreement on number of files";

		num_files_seen++;
	}
	CHECK_NE(num_files_seen, 0) << "empty directory";
	CHECK_EQ(num_files, num_files_seen)
		<< "number of files in directory disagrees with filename";

	openFile();
}

void DatasetParser::openFile() {
	if (csv_file.is_open()) {
		csv_file.close();
	}
	std::string fname = str(boost::format("part-%5u-of-%5u.csv")
							% current_index % num_files);
	VLOG(1) << "Opening " << fname;
	std::string path = (dataset_path / fname).string();
	csv_file.open(path);
}

bool DatasetParser::nextRow() {
	std::string line;
	if (getline(csv_file, line)) {
		VLOG(3) << "Processing " << line;
		boost::split(values, line, boost::is_any_of(","),
				     boost::token_compress_off);
		return true;
	}

	// EOF in file
	current_index++;
	if (current_index >= num_files) {
		// end of dataset
		return false;
	}

	// open next input file
	openFile();
	return nextRow();
}

DatasetParser::~DatasetParser() { }

bool MachineParser::nextRow() {
	bool success = DatasetParser::nextRow();
	if (success) {
		// attempt to parse
		if (values.size() != 6) {
			LOG(ERROR) << "Unexpected structure of machine event row";
			return false;
		}
		machine.timestamp = boost::lexical_cast<uint64_t>(values[0]);
		machine.machine_id = boost::lexical_cast<uint64_t>(values[1]);
		machine.event_type = boost::lexical_cast<unsigned int>(values[2]);
	}
	return success;
}

bool JobParser::nextRow() {
	bool success = DatasetParser::nextRow();
	if (success) {
		// attempt to parser
		if (values.size() != 8) {
			LOG(ERROR) << "Unexpected structure of job event row";
			return false;
		}
		job.timestamp = boost::lexical_cast<uint64_t>(values[0]);
		job.job_id = boost::lexical_cast<uint64_t>(values[2]);
		job.event_type = boost::lexical_cast<unsigned int>(values[3]);
	}
	return success;
}

bool TaskParser::nextRow() {
	bool success = DatasetParser::nextRow();
	if (success) {
		// attempt to parser
		if (values.size() != 13) {
			LOG(ERROR) << "Unexpected structure of task event row";
			return false;
		}
		task.timestamp = boost::lexical_cast<uint64_t>(values[0]);
		task.job_id = boost::lexical_cast<uint64_t>(values[2]);
		task.event_type = boost::lexical_cast<unsigned int>(values[5]);
	}
	return success;
}

} /* namespace sim */
} /* namespace firmament */
