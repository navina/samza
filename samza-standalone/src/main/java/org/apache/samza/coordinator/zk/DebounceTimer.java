package org.apache.samza.coordinator.zk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DebounceTimer {
  public static final Logger LOGGER = LoggerFactory.getLogger(DebounceTimer.class);

  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newScheduledThreadPool(1);
  private ScheduledFuture futureHandle = null;
  private final int debounceTimeMs;
  private final ReadyToCreateJobModelListener listener;

  DebounceTimer (int debounceTimeMs, final ReadyToCreateJobModelListener listener) {
    this.debounceTimeMs = debounceTimeMs;
    this.listener = listener;
  }

  // Invoked when group membership changes
  void resetTimer() {
    if (futureHandle != null && !futureHandle.isDone()) {
      boolean cancelResponse = futureHandle.cancel(false);
      if (!cancelResponse) {
        LOGGER.warn("Timer reset may not have cancelled JobModel generation. Expect another round of JobModel re-generation");
      }
    }
    futureHandle = scheduledExecutorService.schedule(
        new Runnable() {
          @Override
          public void run() {
            listener.readyForJobModelCreation();
          }
        },
        debounceTimeMs,
        TimeUnit.MILLISECONDS
    );

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
    scheduledExecutorService.shutdown();
  }

}
