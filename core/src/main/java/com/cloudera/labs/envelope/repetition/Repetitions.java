/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.run.DataStep;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class to keep track of repeating steps and manage a shared scheduled executor pool. We use a ReentrantLock here
 * to avoid both Runner and Repetition instances modifying the HashSet simultaneously from addRepeatingStep and
 * getAndClearRepeatingSteps.
 */
public class Repetitions {

  private static final Logger LOG = LoggerFactory.getLogger(Repetitions.class);

  private static Repetitions repetitions;

  // Keep a list of steps that need reloading
  private Set<DataStep> reloadSteps = Sets.newHashSet();
  private Lock lock = new ReentrantLock();

  // A shared scheduled executor pool
  private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);
  private ConcurrentLinkedQueue<ScheduledFuture> runningTasks = new ConcurrentLinkedQueue<>();

  private Repetitions() {
  }

  /**
   * Add a step for repetition. Up until the {@link #getAndClearRepeatingSteps()} method is next called, adding the
   * same Step instance multiple times is idempotent.
   * @param step
   */
  public synchronized void addRepeatingStep(DataStep step) {
    lock.lock();
    try {
      reloadSteps.add(step);
      LOG.debug("Added step [{}] to reloadSteps (size [{}])", step.getName(), reloadSteps.size());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieve a copy of the set of step instances that should be repeated and empty the set.
   * @return a {@link Set} of {@link DataStep} instances to be repeated
   */
  public synchronized Set<DataStep> getAndClearRepeatingSteps() {
    Set<DataStep> tempSteps = Sets.newHashSet();
    lock.lock();
    try {
      tempSteps.addAll(reloadSteps);
      reloadSteps.clear();
    } finally {
      lock.unlock();
    }
    return tempSteps;
  }

  /**
   * Add a {@link Runnable} task to be run at a set interval. The first invocation of the task will occur approximately
   * {@code interval} milliseconds after calling this method.
   * @param task {@link Runnable} instance to be run at a regular interval
   * @param interval the interval in milliseconds at which the task should run
   * @return a {@link ScheduledFuture} a ScheduledFuture representing pending completion of the task or null if the task
   * could not be submitted
   */
  public ScheduledFuture submitRegularTask(Runnable task, long interval) {
    ScheduledFuture future = null;
    if (!executorService.isShutdown() && !executorService.isTerminated()) {
      future = executorService.scheduleAtFixedRate(task, interval, interval, TimeUnit.MILLISECONDS);
      runningTasks.add(future);
    }
    return future;
  }

  /**
   * Shutdown all currently scheduled tasks. Tasks are stopped by calling the {@code ScheduledFuture.cancel(true)}
   * method meaning that running tasks will be interrupted. After calling this method new tasks cannot be submitted and
   * a new {@code Repetitions} instance should be created with the {@see get(true)} method.
   */
  public void shutdownTasks() {
    for (ScheduledFuture future : runningTasks) {
      future.cancel(true);
    }
    runningTasks.clear();
    executorService.shutdownNow();
  }

  /**
   * Get the singleton {@code Repetitions} instance
   * @return the {@code Repetitions} instance
   */
  public static Repetitions get() {
    return get(false);
  }

  /**
   * Get the singleton {@code Repetitions} instance or create a new one after shutting down the old instance.
   * @param reload should create a new instance
   * @return the existing or new {@code Repetitions} instance
   */
  public static synchronized Repetitions get(boolean reload) {
    if (repetitions == null) {
      LOG.debug("New Repetitions instance");
      repetitions = new Repetitions();
    } else if (reload) {
      LOG.debug("Reloading Repetitions");
      repetitions.shutdownTasks();
      repetitions = new Repetitions();
    }
    return repetitions;
  }

}
