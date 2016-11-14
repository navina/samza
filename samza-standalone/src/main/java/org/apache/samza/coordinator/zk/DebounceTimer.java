package org.apache.samza.coordinator.zk;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DebounceTimer {
  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newScheduledThreadPool(1);
  private ScheduledFuture futureHandle = null;
  private final int debounceTimeMs;
  private List<String> memberList = null;
  private final ReadyToCreateJobModelListener listener;

  DebounceTimer (int debounceTimeMs, List<String> initialMemberList, final ReadyToCreateJobModelListener listener) {
    this.debounceTimeMs = debounceTimeMs;
    this.memberList = initialMemberList;
    this.listener = listener;
    // Schedule delayed task
    this.futureHandle = scheduledExecutorService.schedule(
        new Runnable() {
          @Override
          public void run() {
            listener.readyForJobModelCreation();
          }
        },
        debounceTimeMs,
        TimeUnit.MILLISECONDS
    );

  }

  // Invoked when group membership changes
  void resetTimer() {
   /*
   if (membership changed from prevMemberList) {
    if (alreadyScheduledTask) {
      interrupt even if it is running
    }
    schedule same task again with debounce time delay

    }
    Task:: call job model generator
       */

  }

  void stopTimer() {
    // shutdown executor service
  }

}
