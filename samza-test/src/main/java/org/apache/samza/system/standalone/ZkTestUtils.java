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
package org.apache.samza.system.standalone;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.ZkConfig;


public class ZkTestUtils {
  private static final String ZK_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";
<<<<<<< HEAD
<<<<<<< HEAD
//  private static final String STANDALONE_SSP_GROUPER_FACTORY = "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory";
=======
    private static final String STANDALONE_SSP_GROUPER_FACTORY = "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory";
>>>>>>> JobModelPublish
//  private static final String STANDALONE_TASK_NAME_GROUPER_FACTORY = "org.apache.samza.container.grouper.task.SingleContainerGrouperFactory";
=======
>>>>>>> ZkTestUtils
  private static final String KAFKA_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";

  public static final String JOB_NAME = "job.name";
  private static final String TASK_CLASS = "task.class";
<<<<<<< HEAD
  private static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";
<<<<<<< HEAD
//  private static final String TASK_NAME_GROUPER_FACTORY = "task.name.grouper.factory";
=======
  private static final String TASK_NAME_GROUPER_FACTORY = "task.name.grouper.factory";
>>>>>>> JobModelPublish
//  private static final String SSP_GROUPER_FACTORY = "job.systemstreampartition.grouper.factory";
=======
>>>>>>> ZkTestUtils

  private static final String ZOOKEEPER_CONNECT_FORMAT_STRING = "systems.%s.consumer.zookeeper.connect";
  private static final String BOOTSTRAP_SERVERS_FORMAT_STRING = "systems.%s.producer.bootstrap.servers";
  private static final String SERIALIZERS_REGISTRY_FORMAT_STRING = "serializers.registry.%s.class";
  private static final String SYSTEM_FACTORY_FORMAT_STRING = "systems.%s.samza.factory";
  private static final String SYSTEM_OFFSET_DEFAULT_FORMAT_STRING = "systems.%s.samza.offset.default";
  private static final String OFFSET_OLDEST = "oldest";

  public enum SerdeAlias {
    STRING,
    INT
  }
  private ZkTestUtils() {

  }

  public static Map<String, String> getZkConfigs(String jobName, String taskClass, String zkConnect) {
    return new HashMap<String, String>() {
      {
        put(JOB_NAME, jobName);
        put(TASK_CLASS, taskClass);
        put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, ZK_JOB_COORDINATOR_FACTORY);
        put(ZkConfig.ZK_CONNECT, zkConnect);
<<<<<<< HEAD
//        put(SSP_GROUPER_FACTORY, STANDALONE_SSP_GROUPER_FACTORY);
//        put(TASK_NAME_GROUPER_FACTORY, STANDALONE_TASK_NAME_GROUPER_FACTORY);
=======
>>>>>>> ZkTestUtils
      }
    };
  }

  private static Map<String, String> getSerdeConfigs(String systemName, String keyOrMsg, SerdeAlias serde) {
    Map<String, String> result = new HashMap<>();
    switch (serde) {
      case STRING:
        result.put(
            String.format(SERIALIZERS_REGISTRY_FORMAT_STRING, "string"),
            "org.apache.samza.serializers.StringSerdeFactory");
        result.put(
            String.format("systems.%s.samza.%s.serde", systemName, keyOrMsg),
            "string");
        break;
      case INT:
        result.put(
            String.format(SERIALIZERS_REGISTRY_FORMAT_STRING, "int"),
            "org.apache.samza.serializers.IntegerSerdeFactory");
        result.put(
            String.format("systems.%s.samza.%s.serde", systemName, keyOrMsg),
            "int");
        break;
    }
    return result;
  }

  public static Map<String, String> getKafkaSystemConfigs(
      String systemName,
      String bootstrapServers,
      String zkConnection,
      SerdeAlias keySerde,
      SerdeAlias msgSerde,
      boolean resetOffsetToEarliest) {
    Map<String, String> result = new HashMap<>();
    result.put(String.format(SYSTEM_FACTORY_FORMAT_STRING, systemName), KAFKA_SYSTEM_FACTORY);
    result.put(String.format(BOOTSTRAP_SERVERS_FORMAT_STRING, systemName), bootstrapServers);
    result.put(String.format(ZOOKEEPER_CONNECT_FORMAT_STRING, systemName), zkConnection);
    if (keySerde != null) {
      result.putAll(getSerdeConfigs(systemName, "key", keySerde));
    }
    if (msgSerde != null) {
      result.putAll(getSerdeConfigs(systemName, "msg", msgSerde));
    }
    if (resetOffsetToEarliest) {
      result.put(String.format(SYSTEM_OFFSET_DEFAULT_FORMAT_STRING, systemName), OFFSET_OLDEST);
    }
    return result;
  }

}
