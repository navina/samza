package org.apache.samza.coordinator.zk;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.HttpJobModelFromLeader;
import org.apache.samza.JobModelListener;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManager$;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ZkCoordinator implements
    JobCoordinator, IZkStateListener, LeaderElector, ReadyToCreateJobModelListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkCoordinator.class);

  private final ZkClient zkClient;
  private final ZkPaths zkPaths;
  private String ephemeralId = null;
  private String currentSubscription = null;
  private ZkLeaderChangeListener zkLeaderChangeListener = new ZkLeaderChangeListener();
  private ZkProcessorMemberChangeListner memberChangeListener = new ZkProcessorMemberChangeListner();
  private JobModelListener jobModelListener = null;
  private String leaderId = null;
  private final Config config;
  private final Random random = new Random();
  public AtomicReference<JobModel> currentJobModel = new AtomicReference<>();

  public ZkCoordinator (String jobName, String jobId, String zkConnectString, Config config) {
    this.zkClient = new ZkClient(zkConnectString, 60000); // Make timeout configurable
    this.zkPaths = new ZkPaths(jobName, jobId);
    this.config = config;
  }

  @Override
  public void start() {
    zkClient.waitUntilConnected(60000, TimeUnit.MILLISECONDS);
    zkClient.subscribeStateChanges(this);

    try {
      if (!zkClient.exists(zkPaths.getProcessorsPath())) {
        zkClient.createPersistent(zkPaths.getProcessorsPath(), true);
      }
    } catch (ZkNodeExistsException e) {
      // Ignore
    } catch (Exception ee) {
      throw new SamzaException("Unknown exception from Zookeeper when creating processor path", ee);
    }
    try {
      ephemeralId = zkClient.createEphemeralSequential(zkPaths.getProcessorsPath() + "/processor-", "hostname");
    } catch (Exception e) {
      throw new SamzaException("Couldn't register with Zk! Failing out - TODO: Add retry");
    }

    join();
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

  @Override
  public void onBecomeLeader() {
    LOGGER.info("Becoming Leader - unsubscribe to leader path");
    zkClient.unsubscribeDataChanges(zkPaths.getLeaderPath(), zkLeaderChangeListener);
    zkClient.createEphemeral(zkPaths.getLeaderPath(), ephemeralId);
    LOGGER.info("Became leader.. Subscribing to member changes");
    zkClient.subscribeChildChanges(zkPaths.getProcessorsPath(), memberChangeListener);

    // Wait for Debounce Timer to Expire before generating JobModel
    this.currentJobModel.getAndSet(generateJobModel());
    jobModelListener.onJobModelUpdate(this.currentJobModel.get());
  }

  private JobModel generateJobModel() {
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    systemAdmins.put("kafka", new KafkaSystemFactory().getAdmin("kafka", this.config));


    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());
    JobModelManager jc = JobModelManager$.MODULE$.getJobCoordinator(this.config, null, null, streamMetadataCache, null);
    return jc.jobModel();
  }

  @Override
  public void setJobModelChangeListener(JobModelListener listener) {
    jobModelListener = listener;
  }

  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

  }

  @Override
  public void handleNewSession() throws Exception {

  }

  private boolean amILeader() {
    return ephemeralId.equals(leaderId);
  }

  @Override
  public void join() {
    if (ephemeralId == null) {
      LOGGER.error("Not registered with live processors - /processors. Cannot join LE without registration!");
      throw new RuntimeException("Something went wrong when registering with zk"); // TODO: add retry
    }

    List<String> children = zkClient.getChildren(zkPaths.getProcessorsPath());
    assert children.size() > 0;
    Collections.sort(children);
    LOGGER.info("Found these children - " + children);

    String id = ephemeralId.substring(ephemeralId.lastIndexOf("/")+1);
    int index = children.indexOf(id);

    if (index == -1) {
      LOGGER.info("Looks like we lost connection with ZK!! Not handling reconnect for now");
      throw new RuntimeException("Looks like we lost connection with ZK!! Not handling reconnect for now");
    }

    if (index == 0) {
      leaderId = ephemeralId;
      onBecomeLeader();
    } else {
      LOGGER.info("Not eligible for leader. Waiting for Leader to be elected");
      String prevCandidate = children.get(index - 1);
      if (!prevCandidate.equals(currentSubscription)) {
        if (currentSubscription != null) {
          zkClient.unsubscribeDataChanges(zkPaths.getProcessorsPath() + "/" + currentSubscription, zkLeaderChangeListener);
        }
        currentSubscription = prevCandidate;
        LOGGER.info("Subscribing to " +  currentSubscription);
        zkClient.subscribeDataChanges(zkPaths.getProcessorsPath() + "/" + currentSubscription, zkLeaderChangeListener);
      }

      // Double check that previous candidate exists
      boolean prevCandidateExists = zkClient.exists(zkPaths.getProcessorsPath() + "/" + currentSubscription);
      if (prevCandidateExists) {
        LOGGER.info("Previous candidate still exists. Let's wait for it to away!");
      } else {
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        join();
      }
    }
  }

  @Override
  public void resign() {

  }

  @Override
  public void readyForJobModelCreation() {
    // Create JobModel
    // Persist it somewhere durable
    // Do something for notification - in ZK, update the version of Jobmodel /jobmodel
  }

  class ZkLeaderChangeListener implements IZkDataListener {
    // For /leader
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOGGER.info("handleDataChange : " + dataPath + " - " + data);
    }

    // For /leader
    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      LOGGER.info("handleDataDeleted : " + dataPath);
    }
  }

  class ZkProcessorMemberChangeListner implements IZkChildListener {
    // For /processors child change
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      LOGGER.info("handleChildChange : Parent- " + parentPath + " Children-" + currentChilds.toString());
      // Reset Debounce Timer - timer.resetTimer();
    }
  }
}
