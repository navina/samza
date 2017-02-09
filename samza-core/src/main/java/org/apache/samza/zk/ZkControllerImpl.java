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
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class ZkControllerImpl implements ZkController {
  private static final Logger LOG = LoggerFactory.getLogger(ZkControllerImpl.class);

  private String processorIdStr;
  private final ZkUtils zkUtils;
  private final ZkListener zkListener;
  private final ZkLeaderElector leaderElector;
  private final ScheduleAfterDebounceTime debounceTimer;

  public ZkControllerImpl (String processorIdStr, ZkUtils zkUtils, ScheduleAfterDebounceTime debounceTimer, ZkListener zkListener) {
    this.processorIdStr = processorIdStr;
    this.zkUtils = zkUtils;
    this.zkListener = zkListener;
    this.leaderElector = new ZkLeaderElector(this.processorIdStr, this.zkUtils);
    this.debounceTimer = debounceTimer;

    init();
  }

  @Override
  public void register() {

    // TODO - make a loop here with some number of attempts.
    // possibly split into two method - becomeLeader() and becomeParticipant()
    boolean isLeader = leaderElector.tryBecomeLeader();
    if(isLeader) {
      listenToProcessorLiveness();

      // register the debounce call under the same action name as processor change, to make sure it will get cancelled
      // if more processors join before the time is up. They both use the same action - onBecomeLeader()
      debounceTimer.scheduleAfterDebounceTime(ScheduleAfterDebounceTime.ON_PROCESSOR_CHANGE,
        ScheduleAfterDebounceTime.DEBOUNCE_TIME_MS, () -> zkListener.onBecomeLeader());   // RECONSIDER MAKING THIS SYNC CALL
    }

    // subscribe to JobModel version updates
    zkUtils.subscribeToJobModelVersionChange(new ZkJobModelVersionChangeHandler(debounceTimer));
  }

  private void init() {
    ZkKeyBuilder keyBuilder = zkUtils.getKeyBuilder();
    zkUtils.makeSurePersistentPathsExists(new String[] {
        keyBuilder.getProcessorsPath(), keyBuilder.getJobModelVersionPath(), keyBuilder.getJobModelPathPrefix()});
  }

  @Override
  public boolean isLeader() {
    return leaderElector.amILeader();
  }

  @Override
  public void notifyJobModelChange(String version) {
    zkListener.onNewJobModelAvailable(version);
  }

  @Override
  public void stop() {
    if (isLeader()) {
      leaderElector.resignLeadership();
    }
    zkUtils.close();
  }

  @Override
  public void listenToProcessorLiveness() {
    zkUtils.subscribeToProcessorChange(new ZkProcessorChangeHandler(debounceTimer));
  }

  @Override
  public String currentJobModelVersion() {
    return zkUtils.getJobModelVersion();
  }

  // Only by Leader
  class ZkProcessorChangeHandler  implements IZkChildListener {
    private final ScheduleAfterDebounceTime debounceTimer;
    public ZkProcessorChangeHandler(ScheduleAfterDebounceTime debounceTimer) {
      this.debounceTimer = debounceTimer;
    }
    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath    The parent path
     * @param currentChilds The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      LOG.info(
          "ZkControllerImpl::ZkProcessorChangeHandler::handleChildChange - Path: " + parentPath + "  Current Children: "
              + currentChilds);
      debounceTimer.scheduleAfterDebounceTime(ScheduleAfterDebounceTime.ON_PROCESSOR_CHANGE,
          ScheduleAfterDebounceTime.DEBOUNCE_TIME_MS, () -> zkListener.onProcessorChange(currentChilds));
    }
  }

  class ZkJobModelVersionChangeHandler implements IZkDataListener {
    private final ScheduleAfterDebounceTime debounceTimer;
    public ZkJobModelVersionChangeHandler(ScheduleAfterDebounceTime debounceTimer) {
      this.debounceTimer = debounceTimer;
    }
    /**
     * called when job model version gets updated
     * @param dataPath
     * @param data
     * @throws Exception
     */
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOG.info("pid=" + processorIdStr + ". Got notification on version update change. path=" + dataPath + "; data="
          + (String) data);

      debounceTimer
          .scheduleAfterDebounceTime(ScheduleAfterDebounceTime.JOB_MODEL_VERSION_CHANGE, 0, () -> notifyJobModelChange((String) data));
    }
    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      throw new SamzaException("version update path has been deleted!.");
    }
  }

  public void shutdown() {
    if(debounceTimer != null)
      debounceTimer.stopScheduler();

    if(zkUtils != null)
      zkUtils.close();
  }
}
