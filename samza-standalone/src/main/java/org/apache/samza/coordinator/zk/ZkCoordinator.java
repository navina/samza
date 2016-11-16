package org.apache.samza.coordinator.zk;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.HttpJobModelFromLeader;
import org.apache.samza.JobModelListener;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.coordinator.GroupMemberChangeListener;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManager$;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ZkCoordinator implements JobCoordinator, ReadyToCreateJobModelListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkCoordinator.class);

  private final ZkClient zkClient;
  private final ZkPaths zkPaths;
  private String ephemeralId = null;

  private JobModelListener jobModelListener = null;
  private final Config config;
  public AtomicReference<JobModel> currentJobModel = new AtomicReference<>();
  private ZkAdapter zkAdapter;
  private volatile boolean amILeader = false;
  private final DebounceTimer debounceTimer;

  private final String httpServerPort;

  public ZkCoordinator (
      String jobName,
      String jobId,
      String zkConnectString,
      Config config,
      String httpServerPort) {  // TODO: Remove HTTP Server
    this.zkAdapter = new ZkAdapter(
        zkConnectString,
        new ZkPaths(jobName, jobId),
        new LeaderElectorListenerImpl(),
        new GroupMemberChangeListenerImpl(),
        new JobModelListenerImpl());
    this.zkClient = new ZkClient(zkConnectString, 60000); // Make timeout configurable
    this.zkPaths = new ZkPaths(jobName, jobId);
    this.config = config;
    this.httpServerPort = httpServerPort;
    this.debounceTimer = new DebounceTimer(3000, this);
  }

  @Override
  public void start() {
    ephemeralId = registerWithZk();
    zkAdapter.tryBecomeLeader();
  }

  private String registerWithZk() {
    return zkAdapter.register();  // zkadapter.register -> should retry and throw exception if it cannot register
  }

  @Override
  public void stop() {
  }
  
  @Override
  public JobModel getJobModel() {
    String leaderHost = zkClient.readData(zkPaths.getLeaderPath(), true);
    if (leaderHost == null) {
      throw new SamzaException("Could not find leader's host in Leader Path!");
    }
    return new HttpJobModelFromLeader(String.format("http://%s:5555/", leaderHost)).readJobModel();
  }


  private JobModel generateJobModel() {
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        throw new SamzaException("A stream uses system " + systemName + " which is missing from the configuration.");
      }
      SystemFactory factory = Util.<SystemFactory>getObj(systemFactoryClassName);
      systemAdmins.put(systemName, factory.getAdmin(systemName, config));
    }

    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());
    JobModelManager jc = JobModelManager$.MODULE$.getJobCoordinator(this.config, null, null, streamMetadataCache, null);
    return jc.jobModel();
  }

  @Override
  public void setJobModelChangeListener(JobModelListener listener) {
    jobModelListener = listener;
  }


  @Override
  public void readyForJobModelCreation() {
    // Verify leadership is still valid??
    // Create JobModel
    currentJobModel.getAndSet(generateJobModel());
    // TODO: Persist it somewhere durable
    // Do something for notification - in ZK, update the version of Jobmodel /jobmodel
    zkAdapter.updateJobModelVersion(String.format("http://localhost:" + httpServerPort + "/"));
  }

  class GroupMemberChangeListenerImpl implements GroupMemberChangeListener {
    @Override
    public void onGroupMemberChange(String memberIdentifier) {
      debounceTimer.resetTimer();
    }
  }

  class LeaderElectorListenerImpl implements LeaderElectorListener {
    @Override
    public void onBecomeLeader() {
      amILeader = true;
      debounceTimer.resetTimer();
      // Start Timer with JobModelGeneratorListener
      // JobModelGeneratorListener - creates and publishes the JobModel
//      generateJobModel();
      // publish job model and notify (if needed)
    }
  }

  class JobModelListenerImpl implements JobModelListener {

    @Override
    public void onJobModelUpdate() {
      if (jobModelListener != null) {
        jobModelListener.onJobModelUpdate();
      }
    }
  }
}
