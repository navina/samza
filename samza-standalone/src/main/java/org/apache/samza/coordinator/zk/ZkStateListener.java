package org.apache.samza.coordinator.zk;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;

public class ZkStateListener implements IZkStateListener {
  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

  }

  @Override
  public void handleNewSession() throws Exception {

  }
}
