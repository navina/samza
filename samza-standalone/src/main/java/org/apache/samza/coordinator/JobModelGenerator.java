package org.apache.samza.coordinator;

import org.apache.samza.job.model.JobModel;

public interface JobModelGenerator {
  JobModel generateJobModel();
}
