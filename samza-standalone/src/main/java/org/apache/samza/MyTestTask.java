package org.apache.samza;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTestTask implements StreamTask {
  public static final Logger LOGGER = LoggerFactory.getLogger(MyTestTask.class);
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    String message = (String) envelope.getMessage();
    int result = Integer.parseInt(message) * -1;
    LOGGER.info("REsult is " + result);

    collector.send(
        new OutgoingMessageEnvelope(
            new SystemStream(envelope.getSystemStreamPartition().getSystem(), "output"),
            null,
            String.valueOf(result)));
  }
}
