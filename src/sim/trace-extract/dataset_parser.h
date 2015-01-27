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
	struct Types {
		enum types_t { ADD, REMOVE, UPDATE };
	};

	// timestamp 64-bit
	uint64_t timestamp;
	// machine ID a UUID, also 64-bit
	uint64_t machine_id;
	Types::types_t event_type;

	// other attributes are:
	// platform ID, CPU capacity, memory capacity
};

class MachineParser : public DatasetParser {
	MachineEvent machine;
public:
	explicit MachineParser(std::string trace_path) :
				  	  	  	    DatasetParser(trace_path, "machine_events") { }
	bool nextRow() override;
	// note the returned Machine reference is invalided upon the next call to
	// nextRow()
	const MachineEvent &getMachine() {
		return machine;
	}
};

struct JobTaskEventTypes {
	enum types_t { SUBMIT, SCHEDULE, EVICT, FAIL, FINISH, KILL, LOST,
			 	 	 	 	   UPDATE_PENDING, UPDATE_RUNNING };
};

struct JobEvent {
	// timestamp 64-bit
	uint64_t timestamp;
	// job ID a UUID, also 64-bit
	uint64_t job_id;
	JobTaskEventTypes::types_t event_type;
};

class JobParser : public DatasetParser {
	JobEvent job;
public:
	explicit JobParser(std::string trace_path) :
					  	  	  	    DatasetParser(trace_path, "job_events") { }
	bool nextRow() override;
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
	// identifier within the job
	uint32_t task_index;
	// machine ID a UUID, also 64-bit
	uint64_t machine_id;
	JobTaskEventTypes::types_t event_type;
};

class TaskParser : public DatasetParser {
	TaskEvent task;
public:
	explicit TaskParser(std::string trace_path) :
						  	  	   DatasetParser(trace_path, "task_events") { }
	bool nextRow() override;
	// note the returned Task reference is invalidated upon the next call
	const TaskEvent &getTask() {
		return task;
	}
};

} /* namespace sim */
} /* namespace firmament */

#endif /* SRC_SIM_TRACE_EXTRACT_DATASET_PARSER_H_ */
