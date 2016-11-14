package org.apache.samza;

import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.job.model.JobModel;

public class HttpJobModelFromLeader implements JobModelReader {
  private final String leaderUrl;
  public HttpJobModelFromLeader (String leaderUrl) {
    this.leaderUrl = leaderUrl;
  }

  // Constructor passes in the leader's http coordinates
  @Override
  public JobModel readJobModel() {
    return SamzaContainer$.MODULE$.readJobModel(leaderUrl, 1000);
  }
}
