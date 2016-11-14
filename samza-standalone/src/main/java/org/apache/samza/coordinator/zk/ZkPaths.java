package org.apache.samza.coordinator.zk;

public class ZkPaths {
  private final String jobName;
  private final String jobId;

  public ZkPaths(String jobName, String jobId) {
    this.jobName = jobName;
    this.jobId = jobId;
  }

  private static final String PROCESSORS_PATH = "/processors";
  public static final String LEADER_PATH = "/leader";

  public String getProcessorsPath() {
    return String.format("/%s-%s%s", jobName, jobId, PROCESSORS_PATH);
  }

  public String getLeaderPath() {
    return String.format("/%s-%s%s", jobName, jobId, LEADER_PATH);
  }
}
