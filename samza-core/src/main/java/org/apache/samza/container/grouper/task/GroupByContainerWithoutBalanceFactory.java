package org.apache.samza.container.grouper.task;

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;

public class GroupByContainerWithoutBalanceFactory implements TaskNameGrouperFactory {
  @Override
  public TaskNameGrouper build(Config config) {
    return new GroupByContainerWithoutBalance(new JobConfig(config).getContainerCount());
  }
}
