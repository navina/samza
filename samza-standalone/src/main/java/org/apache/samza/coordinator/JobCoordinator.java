package org.apache.samza.coordinator;

import org.apache.samza.JobModelListener;
import org.apache.samza.job.model.JobModel;

/**
 *  A JobCoordinator is a pluggable module in each process that provides the JobModel and the ID to the StreamProcessor.
 *  In some cases, ID assignment is completely config driven, while in other cases, ID assignment may require
 *  coordination with JobCoordinators of other StreamProcessors.
 *  */
public interface JobCoordinator {
  /**
   * Starts the JobCoordinator which involves one or more of the following:
   * * LeaderElector Module initialization, if any
   * * If leader, generate JobModel. Else, read JobModel
   */
  void start();

  /**
   * Cleanly shutting down the JobCoordinator involves:
   * * Shutting down the LeaderElection module (TBD: details depending on leader or not)
   * * TBD
   */
  void stop();

  /**
   * Returns the current JobModel
   * The implementation of the JobCoordinator in the leader needs to know how to read the config and generate JobModel
   * In case of a non-leader, the JobCoordinator should simply fetch the jobmodel
   *
   * @return instance of JobModel that describes the partition distribution among the processors (and hence, tasks)
   */
  JobModel getJobModel();

//  void onBecomeLeader();
  void setJobModelChangeListener (JobModelListener listener);
}