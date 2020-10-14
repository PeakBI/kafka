/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metadata.TopicRecord
import org.apache.kafka.common.protocol.ApiMessage

import scala.collection.mutable

class TopicRecordProcessor extends ApiMessageProcessor {
  override def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessage: ApiMessage): BrokerMetadataBasis = {
    apiMessage match {
      case topicRecord: TopicRecord => process(List(topicRecord), brokerMetadataBasis)
      case unexpected => throw new IllegalArgumentException(s"ApiMessage was not of type TopicRecord: ${unexpected.getClass}")
    }
  }

  override def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessages: List[ApiMessage]): BrokerMetadataBasis = {
    if (apiMessages.isEmpty) {
      return brokerMetadataBasis
    }
    apiMessages.foreach(message => if (!message.isInstanceOf[TopicRecord]) {
      throw new IllegalArgumentException(s"$getClass can only process a list of records when they are all topic records: $apiMessages")
    })
    val topicRecords = apiMessages.asInstanceOf[List[TopicRecord]]
    if (topicRecords.groupBy(tr => tr.name()).valuesIterator.exists(_.size > 1)) {
      throw new IllegalArgumentException(s"$getClass can only process a list of topic records when every topic appears just once: $apiMessages")
    }
    process(topicRecords, brokerMetadataBasis)
  }

  // visible for testing
  private[server] def process(topicRecords: List[TopicRecord], brokerMetadataBasis: BrokerMetadataBasis): BrokerMetadataBasis = {
    val brokerMetadataValue = brokerMetadataBasis.getValue()
    val metadataCacheBasis = brokerMetadataValue.metadataCacheBasis
    val metadataSnapshot = metadataCacheBasis.getValue()
    val partitionStatesCopy = metadataSnapshot.copyPartitionStates()
    val metadataCache = metadataCacheBasis.metadataCache
    val traceEnabled = metadataCache.stateChangeTraceEnabled()
    val topicsGroupedByDeleting = topicRecords.groupBy(tr => tr.deleting())
    topicsGroupedByDeleting.get(false).foreach(addedTopics => {
      addedTopics.foreach(topicRecord => {
        val topicName = topicRecord.name()
        val currentPartitionStatesForTopic = partitionStatesCopy.get(topicName)
        if (currentPartitionStatesForTopic.isDefined) {
          throw new IllegalStateException(s"Saw a new topic that already exists: $topicName")
        }
        partitionStatesCopy(topicName) = mutable.LongMap.empty
        if (traceEnabled) {
          metadataCache.logStateChangeTrace(s"Will cache new topic $topicName with no partitions (yet) via metadata log")
        }
      })
    })
    val deletedTopics = topicsGroupedByDeleting.get(true)
    if (deletedTopics.isEmpty) {
      val newMetadataCacheBasis = metadataCacheBasis.newBasis(
        MetadataSnapshot(partitionStatesCopy, metadataSnapshot.controllerId, metadataSnapshot.aliveBrokers, metadataSnapshot.aliveNodes))
      brokerMetadataBasis.newBasis(brokerMetadataValue.newValue(newMetadataCacheBasis))
    } else {
      var allDeletedPartitions = List[TopicPartition]()
      deletedTopics.foreach(deletedTopics => {
        deletedTopics.foreach(topicRecord => {
          val topicName = topicRecord.name()
          val currentPartitionStatesForTopic = partitionStatesCopy.get(topicName)
          if (currentPartitionStatesForTopic.isEmpty) {
            throw new IllegalStateException(s"Saw a topic being deleted that doesn't exist: $topicName")
          }
          val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
          val partitionsToDelete = currentPartitionStatesForTopic.get.keySet
          partitionsToDelete.foreach(partition => {
            val tp = new TopicPartition(topicName, partition.toInt)
            metadataCache.removePartitionInfo(partitionStatesCopy, topicName, tp.partition())
            if (traceEnabled)
              metadataCache.logStateChangeTrace(s"Will delete partition $tp from metadata cache in response to a TopicRecord on the metadata log")
            deletedPartitions += tp
            allDeletedPartitions = allDeletedPartitions ++ deletedPartitions.toList
          })
        })
      })
      if (traceEnabled)
        metadataCache.logStateChangeTrace(s"Will delete ${allDeletedPartitions.size} partitions from metadata cache in response to TopicRecord(s) in metadata log")
      val newMetadataCacheBasis = metadataCacheBasis.newBasis(
        MetadataSnapshot(partitionStatesCopy, metadataSnapshot.controllerId, metadataSnapshot.aliveBrokers, metadataSnapshot.aliveNodes))
      val newGroupCoordinatorPartitionsDeleted = brokerMetadataValue.groupCoordinatorPartitionsDeleted.addPartitionsDeleted(allDeletedPartitions)
      val newUpdateClientQuotaCallbackMetricConfigs = brokerMetadataValue.updateClientQuotaCallbackMetricConfigs.enableConfigUpdates()
      brokerMetadataBasis.newBasis(
        brokerMetadataValue.newValue(newMetadataCacheBasis)
          .newValue(newGroupCoordinatorPartitionsDeleted)
          .newValue(newUpdateClientQuotaCallbackMetricConfigs))
    }
  }
}
