package com.github.silaev.mongodb.replicaset.examples;

import com.github.silaev.mongodb.replicaset.MongoDbReplicaSet;
import com.github.silaev.mongodb.replicaset.examples.util.MongoDBConnectionUtils;
import com.github.silaev.mongodb.replicaset.examples.util.WaitUtils;
import com.github.silaev.mongodb.replicaset.model.MongoNode;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static com.github.silaev.mongodb.replicaset.model.ReplicaSetMemberState.DOWN;
import static com.github.silaev.mongodb.replicaset.model.ReplicaSetMemberState.PRIMARY;
import static com.github.silaev.mongodb.replicaset.model.ReplicaSetMemberState.SECONDARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FailureTest {
  private static final org.slf4j.Logger LOGGER =
    org.slf4j.LoggerFactory.getLogger(FailureTest.class);

  @Test
  void shouldTestFailures() {
    // GIVEN
    try (
      final MongoDbReplicaSet mongoReplicaSet = MongoDbReplicaSet.builder()
        .mongoDockerImageName("mongo:4.4.4")
        .useHostDockerInternal(true)
        .addToxiproxy(true)
        .replicaSetNumber(3)
        .commandLineOptions(Arrays.asList("--oplogSize", "50"))
        .build()
    ) {
      mongoReplicaSet.start();
      final String replicaSetUrl = mongoReplicaSet.getReplicaSetUrl();
      assertThat(
        mongoReplicaSet.nodeStates(mongoReplicaSet.getMongoRsStatus().getMembers())
      ).containsExactlyInAnyOrder(PRIMARY, SECONDARY, SECONDARY);
      try (
        final MongoClient mongoSyncClient = MongoClients.create(
          MongoDBConnectionUtils.getMongoClientSettingsWithTimeout(
            replicaSetUrl, WriteConcern.MAJORITY.withJournal(true), ReadConcern.MAJORITY, 5
          )
        )
      ) {
        final MongoCollection<Document> collection = getCollection(mongoSyncClient);
        insertDoc("before failure 1", collection);
        final MongoNode masterNodeBeforeFailure1 =
          mongoReplicaSet.getMasterMongoNode(mongoReplicaSet.getMongoRsStatus().getMembers());

        // WHEN: Fault tolerance
        mongoReplicaSet.disconnectNodeFromNetwork(masterNodeBeforeFailure1);
        mongoReplicaSet.waitForMasterReelection(masterNodeBeforeFailure1);
        assertThat(
          mongoReplicaSet.nodeStates(mongoReplicaSet.getMongoRsStatus().getMembers())
        ).containsExactlyInAnyOrder(PRIMARY, SECONDARY, DOWN);
        insertDoc("after failure 1", collection);

        // WHEN: Becoming read only
        final MongoNode masterNodeBeforeFailure2 = mongoReplicaSet.getMasterMongoNode(
          mongoReplicaSet.getMongoRsStatus().getMembers()
        );
        mongoReplicaSet.disconnectNodeFromNetwork(masterNodeBeforeFailure2);
        mongoReplicaSet.waitForMongoNodesDown(2);
        // THEN
        assertThatThrownBy(
          () -> insertDocExceptionally("after failure 2", collection)
        ).isInstanceOf(MongoException.class);
        assertThat(
          mongoReplicaSet.nodeStates(mongoReplicaSet.getMongoRsStatus().getMembers())
        ).containsExactlyInAnyOrder(SECONDARY, DOWN, DOWN);

        // WHEN: Bring all the disconnected nodes back
        mongoReplicaSet.connectNodeToNetwork(masterNodeBeforeFailure1);
        mongoReplicaSet.connectNodeToNetwork(masterNodeBeforeFailure2);
        mongoReplicaSet.waitForAllMongoNodesUp();
        mongoReplicaSet.waitForMaster();
        // THEN
        assertThat(
          mongoReplicaSet.nodeStates(mongoReplicaSet.getMongoRsStatus().getMembers())
        ).containsExactlyInAnyOrder(PRIMARY, SECONDARY, SECONDARY);
        long totalDocs = WaitUtils.pollUntilExpectedOrLatestValue(
          collection::countDocuments,
          3L,
          Duration.ofSeconds(10),
          4
        );
        LOGGER.debug("totalDocs: {}", totalDocs);
        assertThat(totalDocs).satisfiesAnyOf(
          v -> assertThat(v).isEqualTo(2L),
          v -> assertThat(v).isEqualTo(3L)
        );
      }
    }
  }

  private MongoCollection<Document> getCollection(final MongoClient mongoSyncClient) {
    final String dbName = "test";
    final String collectionName = "foo";
    return mongoSyncClient.getDatabase(dbName).getCollection(collectionName);
  }

  private void insertDoc(final String key, final MongoCollection<Document> collection) {
    try {
      insertDocExceptionally(key, collection);
    } catch (MongoException e) {
      LOGGER.debug("Failed to insert a doc with a key: {}, exception: {}", key, e.getMessage());
    }
  }

  private void insertDocExceptionally(
    final String key,
    final MongoCollection<Document> collection
  ) {
    collection.insertOne(new Document(key, Instant.now()));
  }
}
