package org.apache.samza.coordinator.zk;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.JobModelListener;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.GroupMemberChangeListener;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ZkAdapter {
  public static final Logger LOGGER = LoggerFactory.getLogger(ZkAdapter.class);

  // ZkClient and session
  private ZkClient zkClient;
  private final ZkPaths zkPaths;
  private final ZkSessionHandler zkSessionHandler = new ZkSessionHandler();
  // Zk based Leader Election Handlers
  private final ZkLeaderElectorHandler zkLeaderElectorHandler = new ZkLeaderElectorHandler();
  private String currentPredecessor = null;
  private final Random random = new Random();

  private final LeaderElectorListener leaderElectorListener;
  private final GroupMemberChangeListener groupMemberChangeListener;
  private String ephemeralId = null;

  // ZK JobModel Handlers
  private  final ZkJobModelVersionChangeHandler jmVersionHandler = new ZkJobModelVersionChangeHandler();
  private final JobModelListener jobModelListener;

  public ZkAdapter (
      String zkConnectionString,
      ZkPaths zkPaths,
      LeaderElectorListener leaderElectorListener,
      GroupMemberChangeListener groupMemberChangeListener,
      JobModelListener jobModelListener) {
    this.zkClient = new ZkClient(zkConnectionString, 120000, 6000);
    this.leaderElectorListener = leaderElectorListener;
    this.groupMemberChangeListener = groupMemberChangeListener;
    this.jobModelListener = jobModelListener;
    this.zkPaths = zkPaths;
  }

  String register () {
    zkClient.waitUntilConnected(60000, TimeUnit.MILLISECONDS);
    zkClient.subscribeStateChanges(zkSessionHandler);

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

    return ephemeralId;
  }

  void tryBecomeLeader() {
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
      // Add participant change listener
      LOGGER.info("Became leader.. Subscribing to member changes");
      zkClient.subscribeChildChanges(zkPaths.getProcessorsPath(), new ZkParticipantChangeHandler());
      leaderElectorListener.onBecomeLeader();
    } else {
      LOGGER.info("Not eligible for leader. Waiting for Leader to be elected");
      String prevCandidate = children.get(index - 1);
      if (!prevCandidate.equals(currentPredecessor)) {
        if (currentPredecessor != null) {
          zkClient.unsubscribeDataChanges(zkPaths.getProcessorsPath() + "/" + currentPredecessor, zkLeaderElectorHandler);
        }
        currentPredecessor = prevCandidate;
        LOGGER.info("Subscribing to " + currentPredecessor);
        zkClient.subscribeDataChanges(zkPaths.getProcessorsPath() + "/" + currentPredecessor, zkLeaderElectorHandler);
      }

      // Double check that previous candidate exists
      boolean prevCandidateExists = zkClient.exists(zkPaths.getProcessorsPath() + "/" + currentPredecessor);
      if (prevCandidateExists) {
        LOGGER.info("Previous candidate still exists. Let's wait for it to away!");
      } else {
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        tryBecomeLeader();
      }
    }
    zkClient.subscribeDataChanges(zkPaths.getJobModelVersionPath(), jmVersionHandler);
  }

  // Session interface
  class ZkSessionHandler implements IZkStateListener {

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

    }

    @Override
    public void handleNewSession() throws Exception {

    }
  }

  // Participant Change Interface /jobGroup/processors childChangeListener
  class ZkParticipantChangeHandler implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      // Find out the event & Log
      groupMemberChangeListener.onGroupMemberChange("");
    }
  }

  // Previous processor watcher - to trigger leader election (NOT used by LEADER)
  class ZkLeaderElectorHandler implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      // Don't do anything
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // tryBecomeLeader??
      zkClient.unsubscribeDataChanges(zkPaths.getProcessorsPath() + "/" + currentPredecessor, this);  // Is this needed here?
      tryBecomeLeader();
    }
  }

  class ZkJobModelVersionChangeHandler implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      // trigger update job model?
      jobModelListener.onJobModelUpdate();
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // resign
      // stop container??
    }
  }
  void updateJobModelVersion (String version) {
    zkClient.createPersistent(zkPaths.getJobModelVersionPath(), true);
    zkClient.createPersistent(zkPaths.getJobModelVersionPath(), version);
  }
}
