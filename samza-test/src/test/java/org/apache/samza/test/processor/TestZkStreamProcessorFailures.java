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

package org.apache.samza.test.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.zk.TestZkUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Failure tests:
 * ZK unavailable.
 * One processor fails in process.
 */
public class TestZkStreamProcessorFailures extends TestZkStreamProcessorBase {

  private final static int BAD_MESSAGE_KEY = 1000;

  @Before
  public void setUp() {
    super.setUp();
  }

  @Test(expected = org.apache.samza.SamzaException.class)
  public void testZkUnavailable() {
    map.put(ZkConfig.ZK_CONNECT, "localhost:2222"); // non-existing zk
    map.put(ZkConfig.ZK_CONNECTION_TIMEOUT_MS, "3000"); // shorter timeout
    CountDownLatch startLatch = new CountDownLatch(1);
    createStreamProcessor("1", map, startLatch, null); // this should fail with timeout exception
    Assert.fail("should've thrown and exception");
  }

  @Test
  // test with a single processor failing
  // One processor fails (to simulate the failure we inject a special message (id > 1000) which causes the processor to
  // throw an exception.
  public void testFailStreamProcessor() {
    final int numBadMessages = 4; // either of these bad messages will cause p1 to throw and exception
    map.put(JobConfig.JOB_DEBOUNCE_TIME_MS(), "100");

    // set number of events we expect to read by both processes in total:
    // p1 will read messageCount/2 messages
    // p2 will read messageCount/2 messages
    // numBadMessages "bad" messages will be generated
    // p2 will read 2 of the "bad" messages
    // p1 will fail on the first of the "bad" messages
    // a new job model will be generated
    // and p2 will read all 2 * messageCount messages again, + numBadMessages (all of them this time)
    // total 2 x messageCount / 2 + numBadMessages/2 + 2 * messageCount + numBadMessages
    int totalEventsToBeConsumed = 3 * messageCount + numBadMessages + numBadMessages / 2;

    TestStreamTask.endLatch = new CountDownLatch(totalEventsToBeConsumed);
    // create first processor
    Object mutexStart1 = new Object();
    Object mutexStop1 = new Object();
    StreamProcessor sp1 = createStreamProcessor("1", map, mutexStart1, mutexStop1);
    // start the first processor
    Thread t1 = runInThread(sp1, TestStreamTask.endLatch);
    t1.start();

    // start the second processor
    Object mutexStart2 = new Object();
    StreamProcessor sp2 = createStreamProcessor("2", map, mutexStart2, null);
    Thread t2 = runInThread(sp2, TestStreamTask.endLatch);
    t2.start();

    // wait until the processor reports that it has started
    try {
      synchronized (mutexStart1) {
        mutexStart1.wait(1000);
      }
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the first processor to start.");
    }
    // wait until the 2nd processor reports that it has started
    try {
      synchronized (mutexStart2) {
        synchronized (mutexStart2) {
          mutexStart2.wait(1000);
        }
      }
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the 2nd processor to start.");
    }

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // make sure they consume all the messages
    waitUntilConsumedN(totalEventsToBeConsumed - messageCount);

    // produce the bad messages
    produceMessages(BAD_MESSAGE_KEY, inputTopic, 4);

    // wait until the processor reports that it has re-started
    try {
      synchronized (mutexStart2) {
        mutexStart2.wait(1000);
      }
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the first processor to start.");
    }
    // wait for at least one full de-bounce time to let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(200);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until p2 consumes all the message by itself
    waitUntilConsumedN(0);

    // shutdown p2
    try {
      synchronized (t2) {
        t2.notify();
      }
      t2.join(1000);
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished thread:" + e.getLocalizedMessage());
    }

    // number of unique values we gonna read is 0-(2*messageCount - 1) + numBadMessages
    Map<Integer, Boolean> expectedValues = new HashMap<>(2 * messageCount + numBadMessages);
    for (int i = 0; i < 2 * messageCount; i++) {
      expectedValues.put(i, false);
    }
    for (int i = BAD_MESSAGE_KEY; i < numBadMessages + BAD_MESSAGE_KEY; i++) {
      expectedValues.put(i, false);
    }

    verifyNumMessages(outputTopic, expectedValues, totalEventsToBeConsumed);
  }
}
