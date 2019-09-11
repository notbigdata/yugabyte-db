/**
 * Copyright (c) YugaByte, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.yb;

import junit.runner.Version;
import org.junit.internal.JUnitSystem;
import org.junit.internal.RealSystem;
import org.junit.internal.TextListener;
import org.junit.runner.*;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;

import java.util.ArrayList;
import java.util.List;

/**
 * A custom runner that allows to run a particular JUnit test method.
 *
 * Based on some techniques from the JUnitCore command-line tool:
 * https://github.com/junit-team/junit4/blob/master/src/main/java/org/junit/runner/JUnitCore.java
 */
public class YBTestMain extends JUnitCore {

  private static boolean hadErrors = false;

  private static Class getClassByName(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException ex) {
      // The exception message is not very informative -- it is the class name itself, so we skip it.
      System.err.println("Class not found: " + className);
      return null;
    }
  }

  public static void main(String args[]) {
    List<Request> requests = new ArrayList<>();
    for (String arg : args) {
      Class klass = null;
      if (arg.contains("#")) {
        String[] components = arg.split("#");
        if (components.length != 2) {
          System.err.println("Invalid argument: " + arg + " (at most one '#' expected)");
          System.exit(1);
        }
        klass = getClassByName(components[0]);
        if (klass != null) {
          requests.add(Request.method(klass, components[1]));
        }
      } else {
        klass = getClassByName(arg);
        if (klass != null) {
          requests.add(Request.aClass(klass));
        }
      }
      if (klass == null) {
        hadErrors = true;
      }
    }
    if (hadErrors) {
      System.exit(1);
    }


    YBTestMain junitCore = new YBTestMain();
    junitCore.runRequests(requests);

    if (hadErrors) {
      System.exit(1);
    }
  }

  private void runRequests(List<Request> requests) {
    JUnitSystem system = new RealSystem();
    system.out().println("JUnit version " + Version.id());

    JUnitSystem junitSystem = new RealSystem();
    RunNotifier notifier = new RunNotifier();
    RunListener listener = new TextListener(junitSystem);
    notifier.addFirstListener(listener);

    for (Request request : requests) {
      Result result = new Result();
      Runner runner = request.getRunner();
      RunListener resultListener = result.createListener();
      Description runnerDesc = runner.getDescription();
      try {
        notifier.addListener(resultListener);
        system.out().println("Running test: " + runnerDesc + " (test count: " + runner.testCount() + ")");
        notifier.fireTestRunStarted(runner.getDescription());
        runner.run(notifier);
        notifier.fireTestRunFinished(result);
        for (Failure failure : result.getFailures()) {
          notifier.fireTestFailure(failure);
        }
        system.out().println(
            "TEST RESULT: " + runnerDesc +
                "number of tests: " + result.getRunCount() + ", " +
                "failures: " + result.getFailureCount() + ", " +
                "ignored: " + result.getIgnoreCount() + ", " +
                "run time: " + result.getRunTime() + " ms");
        if (!result.wasSuccessful()) {
          hadErrors = true;
        }
      } finally {
        notifier.removeListener(resultListener);
      }
    }
  }
}
