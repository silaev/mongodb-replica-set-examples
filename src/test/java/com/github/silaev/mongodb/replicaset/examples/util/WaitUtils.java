package com.github.silaev.mongodb.replicaset.examples.util;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

public class WaitUtils {
  private WaitUtils() {
  }

  @SuppressWarnings("java:S2925")
  public static long pollUntilExpectedOrLatestValue(
    final Supplier<Long> supplier,
    final long expectedNum,
    final Duration duration,
    final int maxAttempts
  ) {
    int attempt = 0;
    long actualNum = 0;
    while (attempt < maxAttempts) {
      actualNum = supplier.get();
      if (Objects.equals(actualNum, expectedNum)) {
        return actualNum;
      }
      try {
        Thread.sleep(duration.toMillis());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      attempt++;
    }
    return actualNum;
  }
}