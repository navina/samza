package org.apache.samza;

import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.zk.ZkCoordinator;
import org.apache.samza.processor.StreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Driver {
  public static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

  public static void main(String[] args) {
    Map<String, String> mapConfig = new HashMap<String, String>() {
      {
        put("job.name", "zkjob");
        put("job.id", "i001");
        put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
        put("systems.kafka.consumer.zookeeper.connect", "localhost:2181");
        put("systems.kafka.producer.bootstrap.servers", "localhost:9092");
        put("systems.kafka.samza.key.serde", "string");
        put("systems.kafka.samza.msg.serde", "string");
        put("task.inputs", "kafka.numbers");
        put("task.class", "org.apache.samza.MyTestTask");
        put("job.coordinator.system", "kafka");
        put("job.container.count", "2");
        put("job.systemstreampartition.grouper.factory", "org.apache.samza.container.grouper.stream.GroupBySystemStreamPartitionFactory");
        put("task.name.grouper.factory", "org.apache.samza.container.grouper.task.GroupByContainerWithoutBalanceFactory");
      }
    };
    int processorId = Integer.valueOf(args[0]);
    int httpPort = 5555 + processorId;
    JobCoordinator coordinator = new ZkCoordinator("zkjob", "001", "localhost:2181", new MapConfig(mapConfig), String.valueOf(httpPort));
    final StreamProcessor processor = new StreamProcessor(processorId, coordinator);
    processor.start();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        LOGGER.info("Shutting the JVM");
        LOGGER.info("Shutting down the worker!");
        processor.stop();
      }
    }));

    try {
      Thread.sleep(Integer.MAX_VALUE);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      processor.stop();
    }
  }
}
