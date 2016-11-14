package org.apache.samza;

import org.apache.samza.job.model.JobModel;

public interface JobModelListener {
  void onJobModelUpdate(JobModel jobModel);  // Listener for StreamProcessor for JobCoordinator event - when jobmodel is changed
}
