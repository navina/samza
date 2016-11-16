package org.apache.samza.processor;

import org.apache.samza.JobModelListener;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.server.JobServlet;
import org.apache.samza.coordinator.zk.ZkCoordinator;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.JmxServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;

public class StreamProcessor implements JobModelListener {
  JobCoordinator coordinator;
  SamzaContainer container;
  private final int processorId;
  private final HttpServer httpServer;  // For now, all StreamProcessors start an HTTP Server

  public StreamProcessor (int processorId, JobCoordinator coordinator) {
    this.processorId = processorId;
    this.coordinator = coordinator;
    this.httpServer = new HttpServer("/", 5555+processorId, null, new ServletHolder(DefaultServlet.class));
    this.httpServer.addServlet("/*", new JobServlet(((ZkCoordinator) coordinator).currentJobModel));
  }

  @Override
  public void onJobModelUpdate() {
    if (container != null) {
      container.stop();
    }
    JobModel jobModel = coordinator.getJobModel(); // Put Barrier in here?? Blocking call? // Should getJobModel be a part of separate reader interface ??
    container = SamzaContainer.apply(jobModel.getContainers().get(processorId), jobModel, new JmxServer());
    new Thread(new Runnable() {
      @Override
      public void run() {
        container.run();
      }
    }).run();

  }

  public void start() {
    coordinator.setJobModelChangeListener(this);  // Figure out how to set change listener more cleanly ; For now, it has to be set before "start"
    httpServer.start();
    coordinator.start();
  }

  public void stop() {
    coordinator.stop();
    httpServer.stop();
  }
}
