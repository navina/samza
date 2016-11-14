package org.apache.samza.coordinator.zk;

import org.I0Itec.zkclient.IZkDataListener;

// Listening on changes to /jobgroup/leader
public class ZkLeaderListener implements IZkDataListener {

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {

  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {

  }
}
