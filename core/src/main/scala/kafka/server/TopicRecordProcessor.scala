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

import java.util.UUID

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
    // Check if we can process all TopicRecords at the same time.
    // We can only do so as long as any topic name appears only once.
    // This check will no longer be necessary once KIP-516 Topic Identifiers becomes available.
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
    val topicsAdding = topicsGroupedByDeleting.getOrElse(false, List.empty[TopicRecord])
    val topicsDeleting = topicsGroupedByDeleting.getOrElse(true, List.empty[TopicRecord])
    val topicIdMapMaybeCopy: mutable.Map[UUID, String] = if (topicsAdding.isEmpty) metadataSnapshot.topicIdMap else {
      val copy = new mutable.HashMap[UUID, String](metadataSnapshot.topicIdMap.size + topicsAdding.size,
        mutable.HashMap.defaultLoadFactor)
      copy.addAll(metadataSnapshot.topicIdMap)
      copy
    }

    topicsAdding.foreach(topicRecord => {
      val topicId = topicRecord.topicId()
      val topicName = topicRecord.name()
      val currentPartitionStatesForTopic = partitionStatesCopy.get(topicName)
      if (currentPartitionStatesForTopic.isDefined) {
        throw new IllegalStateException(s"Saw a new topic wth a name that already exists: $topicRecord")
      }
      partitionStatesCopy(topicName) = mutable.LongMap.empty
      if (topicIdMapMaybeCopy.contains(topicId)) {
        throw new IllegalStateException(s"Saw a new topic with a topicId that already exists: $topicRecord")
      }
      topicIdMapMaybeCopy(topicId) = topicName
      if (traceEnabled) {
        metadataCache.logStateChangeTrace(s"Will cache new topic $topicId/$topicName with no partitions (yet) via metadata log")
      }
    })
    if (topicsDeleting.isEmpty) {
      val newMetadataCacheBasis = metadataCacheBasis.newBasis(
        MetadataSnapshot(partitionStatesCopy,
          metadataSnapshot.controllerId, metadataSnapshot.aliveBrokers, metadataSnapshot.aliveNodes,
          topicIdMapMaybeCopy))
      brokerMetadataBasis.newBasis(brokerMetadataValue.newValue(newMetadataCacheBasis))
    } else {
      var allDeletingPartitions = Set.empty[TopicPartition]
      topicsDeleting.foreach(topicRecord => {
        val topicName = topicRecord.name()
        val currentPartitionStatesForTopic = partitionStatesCopy.get(topicName)
        if (currentPartitionStatesForTopic.isEmpty) {
          throw new IllegalStateException(s"Saw a topic being deleted that doesn't exist: $topicName")
        }
        val deletingPartitionsForThisTopic = currentPartitionStatesForTopic.get.keySet.map(
          partition => new TopicPartition(topicName, partition.toInt))
        deletingPartitionsForThisTopic.foreach(tp => {
          metadataCache.removePartitionInfo(partitionStatesCopy, topicName, tp.partition())
          if (traceEnabled)
            metadataCache.logStateChangeTrace(s"Will delete partition $tp from metadata cache in response to a TopicRecord on the metadata log")
        })
        allDeletingPartitions = allDeletingPartitions ++ deletingPartitionsForThisTopic
      })
      if (traceEnabled)
        metadataCache.logStateChangeTrace(s"Will delete ${allDeletingPartitions.size} partitions from metadata cache in response to TopicRecord(s) in metadata log")
      val newMetadataCacheBasis = metadataCacheBasis.newBasis(
        MetadataSnapshot(partitionStatesCopy,
          metadataSnapshot.controllerId, metadataSnapshot.aliveBrokers, metadataSnapshot.aliveNodes,
          topicIdMapMaybeCopy))
      val newGroupCoordinatorPartitionsDeleting = brokerMetadataValue.groupCoordinatorPartitionsDeleting.addPartitionsDeleting(allDeletingPartitions)
      val newUpdateClientQuotaCallbackMetricConfigs = brokerMetadataValue.updateClientQuotaCallbackMetricConfigs.enableConfigUpdates()
      brokerMetadataBasis.newBasis(
        brokerMetadataValue.newValue(newMetadataCacheBasis)
          .newValue(newGroupCoordinatorPartitionsDeleting)
          .newValue(newUpdateClientQuotaCallbackMetricConfigs))
    }
  }
}
