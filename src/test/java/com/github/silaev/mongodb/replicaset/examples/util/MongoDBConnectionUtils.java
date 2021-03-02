package com.github.silaev.mongodb.replicaset.examples.util;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;

import java.util.concurrent.TimeUnit;

public class MongoDBConnectionUtils {
  private MongoDBConnectionUtils() {
  }

  /**
   * Gets MongoClientSettings with set timeouts.
   * Note that retryWrites, retryReads are true by default.
   *
   * @param mongoRsUrlPrimary
   * @param writeConcern
   * @param timeout
   * @return
   */
  public static MongoClientSettings getMongoClientSettingsWithTimeout(
    final String mongoRsUrlPrimary,
    final WriteConcern writeConcern,
    final ReadConcern readConcern,
    final int timeout
  ) {
    final ConnectionString connectionString = new ConnectionString(mongoRsUrlPrimary);
    return MongoClientSettings.builder()
      .writeConcern(writeConcern.withWTimeout(timeout, TimeUnit.SECONDS))
      .readConcern(readConcern)
      .applyToClusterSettings(c -> c.serverSelectionTimeout(timeout, TimeUnit.SECONDS))
      .applyConnectionString(connectionString)
      .applyToSocketSettings(
        b -> b
          .readTimeout(timeout, TimeUnit.SECONDS)
          .connectTimeout(timeout, TimeUnit.SECONDS)
      ).build();
  }
}