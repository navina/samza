/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.zk;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.BarrierForVersionUpgradeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


/**
 * This class creates a barrier for version upgrade.
 * Barrier is started by the participant responsible for the upgrade. (start())
 * Each participant will mark its readiness and register for a notification when the barrier is reached. (waitFor())
 * If a timer (started in start()) goes off before the barrier is reached, all the participants will unsubscribe
 * from the notification and the barrier becomes invalid.
 */
public class ZkBarrierForVersionUpgrade implements BarrierForVersionUpgrade {
  private final static Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);

  private final ZkUtils zkUtils;
  private final String barrierPrefix;

  private BarrierForVersionUpgradeListener barrierListener;
  private String barrierDonePath;
  private String barrierProcessors;

  public ZkBarrierForVersionUpgrade(String barrierPrefix, ZkUtils zkUtils, BarrierForVersionUpgradeListener barrierListener) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkBarrierForVersionUpgrade without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.barrierPrefix = barrierPrefix;
    this.barrierListener = barrierListener;
  }

  private void setPaths(String version) {
    String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    barrierDonePath = String.format("%s/barrier_done", barrierPath);
    barrierProcessors = String.format("%s/barrier_processors", barrierPath);
  }

  @Override
  public void start(final String version, List<String> participants) {
    setPaths(version);
    zkUtils.makeSurePersistentPathsExists(new String[]{barrierPrefix, String.format("%s/barrier_%s", barrierPrefix, version), barrierProcessors, barrierDonePath});

    // subscribe for processor's list changes
    LOG.info("Subscribing for child changes at " + barrierProcessors);
    zkUtils.getZkClient().subscribeChildChanges(barrierProcessors, new ZkBarrierChangeHandler(participants));

    if (barrierListener != null) {
      barrierListener.onBarrierCreated(version);
    }
  }

  @Override
  public void joinBarrier(String version, String participantName) {
    setPaths(version);

    final String barrierProcessorThis = String.format("%s/%s", barrierProcessors, participantName);

    // now subscribe for the barrier
    zkUtils.getZkClient().subscribeDataChanges(barrierDonePath, new ZkBarrierReachedHandler(barrierDonePath, version));

    // update the barrier for this processor
    LOG.info("Creating a child for barrier at " + barrierProcessorThis);
    zkUtils.getZkClient().createPersistent(barrierProcessorThis);
  }

  @Override
  public void setBarrierForVersionUpgrade(BarrierForVersionUpgradeListener listener) {
    this.barrierListener = listener;
  }

  public void expireBarrier(String version) {
    zkUtils.getZkClient().writeData(
        barrierDonePath,
        BarrierForVersionUpgrade.State.TIMED_OUT);

  }
  /**
   * Listener for the subscription for the list of participants.
   * This method will mark the barrier as DONE when all the participants have joined.
   */
  class ZkBarrierChangeHandler implements IZkChildListener {
    private final List<String> names;

    public ZkBarrierChangeHandler(List<String> names) {
      this.names = names;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      if (currentChildren == null) {
        LOG.info("Got handleChildChange with null currentChildren");
        return;
      }
      LOG.info("list of children in the barrier = " + parentPath + ":" + Arrays.toString(currentChildren.toArray()));
      LOG.info("list of children to compare against = " + parentPath + ":" + Arrays.toString(names.toArray()));

      // check if all the names are in
      if (CollectionUtils.containsAll(currentChildren, names)) {
        LOG.info("ALl nodes reached the barrier");
        LOG.info("Writing BARRIER DONE to " + barrierDonePath);
        zkUtils.getZkClient().writeData(barrierDonePath, State.DONE); // this will trigger notifications
        zkUtils.getZkClient().unsubscribeChildChanges(barrierDonePath, this);
      }
    }
  }

  class ZkBarrierReachedHandler implements IZkDataListener {
    private final String barrierPathDone;
    private final String barrierVersion;

    public ZkBarrierReachedHandler(String barrierPathDone, String version) {
      this.barrierPathDone = barrierPathDone;
      this.barrierVersion = version;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) {
      LOG.info("got notification about barrier path=" + barrierPathDone + "; done=" + data);
      if (barrierListener != null) {
        barrierListener.onBarrierStateChanged(barrierVersion, (State) data);
      }
        // in any case we unsubscribe
        zkUtils.unsubscribeDataChanges(barrierPathDone, this);
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      LOG.warn("barrier done got deleted at " + dataPath);
    }
  }
}
