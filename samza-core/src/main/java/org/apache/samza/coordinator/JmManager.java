package org.apache.samza.coordinator;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.task.BalancingTaskNameGrouper;
import org.apache.samza.container.grouper.task.TaskNameGrouper;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.ChangelogPartitionManager;
import org.apache.samza.system.ExtendedSystemAdmin;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class JmManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(JmManager.class);

  private final MetricsRegistryMap metricsRegistry;

  private JobModel currentJobModel = null;
  private Config currentConfig = null;
  private final CoordinatorStreamSystemConsumer consumer;
  private final CoordinatorStreamSystemProducer producer;
  private StreamMetadataCache streamMetadataCache = null;

  private final ChangelogPartitionManager changelogPartitionManager;
  private final LocalityManager localityManager;

  // JobModel Information
  private Map<TaskName, Set<SystemStreamPartition>> sspGroups = null;
  private Map<TaskName, Integer> changelogPartitionMapping = null;
  private final TaskNameGrouper taskNameGrouper;

  private StreamPartitionCountMonitor partitionCountMonitor = null; // TODO - Doesn't fit well

  public JmManager (
      Config config,
      CoordinatorStreamSystemConsumer consumer,
      CoordinatorStreamSystemProducer producer,
      MetricsRegistryMap metricsRegistry,
      TaskNameGrouper taskNameGrouper) {
    this.currentConfig = config;
    this.consumer = consumer;
    this.producer = producer;
    this.metricsRegistry = metricsRegistry;
    this.changelogPartitionManager = new ChangelogPartitionManager(producer, consumer, JmManager.class.getName());
    this.localityManager = new LocalityManager(producer, consumer);
    this.taskNameGrouper = taskNameGrouper;
  }

  public void start () {
    // Start ChangelogPartitionManager and LocalityManager
    changelogPartitionManager.start();
    localityManager.start();

    // Parse basic system config
    JavaSystemConfig systemConfig = new JavaSystemConfig(currentConfig);
    List<String> systemNames = systemConfig.getSystemNames();
    Map<String, SystemAdmin> systemAdmins = new HashMap();
    systemNames.stream().forEach(systemName -> {
      String factory = systemConfig.getSystemFactory(systemName);
      if (factory == null) {
        throw new SamzaException("Missing factory configuration for system " + systemName);
      }
      systemAdmins.put(systemName, Util.<SystemFactory>getObj(factory).getAdmin(systemName, currentConfig));
    });

    // Initialize Metadata Cache
    if (streamMetadataCache == null) {
      streamMetadataCache = new StreamMetadataCache(
          Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins),
          0,
          new Clock() {
            @Override
            public long currentTimeMillis() {
              return System.currentTimeMillis();
            }
          });
    }

    // Start consumer & Bootstrap From Coordinator Stream
    LOGGER.info("Registering coordinator system stream.");
    consumer.register();
    LOGGER.debug("Starting coordinator system stream.");
    consumer.start();
    LOGGER.debug("Bootstrapping coordinator system stream.");
    consumer.bootstrap();

    // Register Producer
    LOGGER.info("Registering coordinator system stream producer.");
    producer.register(JmManager.class.getName());

    // Create StreamPartitionCountMonitor, if configured
    JobConfig jobConfig = new JobConfig(currentConfig);
    StreamPartitionCountMonitor streamPartitionCountMonitor = null;
    if (jobConfig.getMonitorPartitionChange()) {
      Map<String, SystemAdmin> extendedSystemAdmins =
          systemAdmins.entrySet().stream()
              .filter(entry -> entry.getValue() instanceof ExtendedSystemAdmin)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      Set<SystemStream> inputStreamsToMonitor = new TaskConfigJava(currentConfig)
          .getAllInputStreams()
          .stream()
          .filter(ss -> extendedSystemAdmins.containsKey(ss.getSystem()))
          .collect(Collectors.toCollection(HashSet::new));

      if (inputStreamsToMonitor.size() > 0) {
        streamPartitionCountMonitor = new StreamPartitionCountMonitor(
            inputStreamsToMonitor,
            streamMetadataCache,
            metricsRegistry,
            jobConfig.getMonitorPartitionChangeFrequency());
      }
    }

    initializeJobModel();
    createChangeLogStreams(currentJobModel.maxChangeLogStreamPartitions);
  }

  private void initializeJobModel() {
    if (currentJobModel == null) {
      JobConfig jobConfig = new JobConfig(currentConfig);
      TaskConfig taskConfig = new TaskConfig(currentConfig);

      SystemStreamPartitionGrouper sspGrouper = jobConfig.getSystemStreamPartitionGrouper();
      Set<SystemStreamPartition> allSsps = JavaConversions.asJavaSet(jobConfig.getMatchedInputStreamPartitions(
          jobConfig.getInputStreamPartitions(
              taskConfig.getInputStreams(),
              streamMetadataCache)
      ));
      sspGroups = sspGrouper.group(allSsps);
      Map<TaskName, Integer> initialChangelogMapping = changelogPartitionManager.readChangeLogPartitionMapping();
      if (initialChangelogMapping.size() == 0) {
        LOGGER.info("Couldn't find any changelog partition mapping. Hence, not setting it.");
      } else {
        changelogPartitionMapping = initialChangelogMapping;
      }
      currentJobModel = refreshJobModel();
      // Save the changelog mapping back to the ChangelogPartitionmanager
      if (changelogPartitionMapping != null && changelogPartitionMapping.size() > 0)
      {
        // newChangelogMapping is the merging of all current task:changelog
        // assignments with whatever we had before (previousChangelogMapping).
        // We must persist legacy changelog assignments so that
        // maxChangelogPartitionId always has the absolute max, not the current
        // max (in case the task with the highest changelog partition mapping
        // disappears.
        currentJobModel
            .getContainers()
            .values()
            .stream()
            .forEach(entry -> {
              entry.getTasks().forEach(
                  (task,tm) -> initialChangelogMapping.put(task, Integer.valueOf(tm.getChangelogPartition().getPartitionId()))
              );
            });
        LOGGER.info("Saving task-to-changelog partition mapping: " + changelogPartitionMapping);
        changelogPartitionManager.writeChangeLogPartitionMapping(changelogPartitionMapping);
      }
    }
  }

  JobModel refreshJobModel() {
    // If no mappings are present(first time the job is running) we return -1, this will allow 0 to be the first change
    // mapping.
    final AtomicInteger maxChangelogPartitionId = new AtomicInteger(-1);
    if (changelogPartitionMapping != null && changelogPartitionMapping.size() > 0) {
      List<Integer> sortedList =
          changelogPartitionMapping.values().stream().map(x -> x.intValue()).sorted().collect(Collectors.toList());
      maxChangelogPartitionId.set(sortedList.get(sortedList.size() - 1));
    }
    // Sort the groups prior to assigning the changelog mapping so that the mapping is reproducible and intuitive
    Map<TaskName, Set<SystemStreamPartition>> sortedGroups =
        new TreeMap<TaskName, Set<SystemStreamPartition>>(sspGroups);

    // Assign all SystemStreamPartitions to TaskNames.
    Set<TaskModel> taskModelSet = new HashSet<>();
    sortedGroups
        .entrySet()
        .stream()
        .forEach(entry -> {
          Integer changelogPartitionId = changelogPartitionMapping.get(entry.getKey());
          if (changelogPartitionId != null) {
            taskModelSet.add(new TaskModel(entry.getKey(), entry.getValue(), new Partition(changelogPartitionId)));
          } else {
            // If we've never seen this TaskName before, then assign it a
            // new changelog.
            int partitionId = maxChangelogPartitionId.incrementAndGet();
            LOGGER.info(String.format(
                "New task %s is being assigned changelog partition %s.",
                entry.getKey(),
                partitionId));
            taskModelSet.add(new TaskModel(entry.getKey(), entry.getValue(), new Partition(partitionId)));
          }
    });

    Set<ContainerModel> containerModelSet;
    if (taskNameGrouper instanceof BalancingTaskNameGrouper) {
      containerModelSet = ((BalancingTaskNameGrouper) taskNameGrouper).balance(taskModelSet, localityManager);
    } else {
      containerModelSet = taskNameGrouper.group(taskModelSet);
    }
    Map<Integer, ContainerModel> containerModelMap = new HashMap<>();
    containerModelSet
        .stream().forEach(containerModel -> containerModelMap.put(Integer.valueOf(containerModel.getContainerId()), containerModel));
    return new JobModel(currentConfig, containerModelMap, localityManager);
  }

  // TODO: Move this out -> should use create topic util in the future
  private void createChangeLogStreams(int changeLogPartitions) {
    JavaStorageConfig storageConfig = new JavaStorageConfig(currentConfig);

    Map<String, SystemStream> changelogSystemStreams = new HashMap<>();
    storageConfig
        .getStoreNames()
        .stream()
        .filter(storeName -> storageConfig.getChangelogStream(storeName) != null)
        .forEach(s -> {
          changelogSystemStreams.put(s, Util.getSystemStreamFromNames(storageConfig.getChangelogStream(s)));
        });
    JavaSystemConfig systemConfig = new JavaSystemConfig(currentConfig);
    changelogSystemStreams
        .forEach((s, ss) -> {
          String systemName = ss.getSystem();
          systemConfig
              .getSystemFactoryObject(
                  systemConfig.getSystemFactory(systemName))
              .getAdmin(ss.getSystem(), currentConfig)
              .createChangelogStream(ss.getStream(), changeLogPartitions);
        });
  }
}
