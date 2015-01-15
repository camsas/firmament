#ifndef SRC_SIM_TRACE_EXTRACT_DATASET_PARSER_H_
#define SRC_SIM_TRACE_EXTRACT_DATASET_PARSER_H_

#include <string>
#include <vector>
#include <fstream>

#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

namespace firmament {
namespace sim {

class DatasetParser {
	fs::path dataset_path;
	std::ifstream csv_file;
	unsigned int current_index, num_files;

	void openFile();
protected:
	std::vector<std::string> values;
public:
	DatasetParser(std::string trace_path, std::string dataset_name);
	// returns false if end of dataset, true otherwise (has read a new value)
	virtual bool nextRow();

	virtual ~DatasetParser();
};

struct MachineEvent {
	// timestamp 64-bit
	uint64_t timestamp;
	// machine ID a UUID, also 64-bit
	uint64_t machine_id;
	// will be small positive integer
	unsigned int event_type;

	// other attributes are:
	// platform ID, CPU capacity, memory capacity

	static const unsigned int ADD_TYPE = 0;
};

class MachineParser : public DatasetParser {
	MachineEvent machine;
public:
	MachineParser(std::string trace_path) :
				  	  	  	    DatasetParser(trace_path, "machine_events") { }
	bool nextRow();
	// note the returned Machine reference is invalided upon the next call to
	// nextRow()
	const MachineEvent &getMachine() {
		return machine;
	}
};

struct JobEvent {
	// timestamp 64-bit
	uint64_t timestamp;
	// job ID a UUID, also 64-bit
	uint64_t job_id;
	// will be small positive integer
	unsigned int event_type;

	static const unsigned int ADD_TYPE = 0;
};

class JobParser : public DatasetParser {
	JobEvent job;
public:
	JobParser(std::string trace_path) :
					  	  	  	    DatasetParser(trace_path, "job_events") { }
	bool nextRow();
	// note the returned Job reference is invalidated upon the next call
	const JobEvent &getJob() {
		return job;
	}
};

struct TaskEvent {
	// timestamp 64-bit
	uint64_t timestamp;
	// job ID a UUID, also 64-bit
	uint64_t job_id;
	// will be small positive integer
	unsigned int event_type;

	static const unsigned int ADD_TYPE = 0;
};

class TaskParser : public DatasetParser {
	TaskEvent task;
public:
	TaskParser(std::string trace_path) :
						  	  	   DatasetParser(trace_path, "task_events") { }
	bool nextRow();
	// note the returned Task reference is invalidated upon the next call
	const TaskEvent &getTask() {
		return task;
	}
};

} /* namespace sim */
} /* namespace firmament */

#endif /* SRC_SIM_TRACE_EXTRACT_DATASET_PARSER_H_ */
