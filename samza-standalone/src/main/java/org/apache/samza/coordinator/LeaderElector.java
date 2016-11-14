package org.apache.samza.coordinator;

public interface LeaderElector {
  void join();
  void resign();
}
