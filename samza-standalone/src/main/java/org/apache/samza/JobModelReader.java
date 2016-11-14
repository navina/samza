package org.apache.samza;

import org.apache.samza.job.model.JobModel;

public interface JobModelReader {
  JobModel readJobModel();  // Read the executable JobModel from a durable store/stream or by querying a service
}
