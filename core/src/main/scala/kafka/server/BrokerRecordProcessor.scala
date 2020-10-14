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

import java.util

import kafka.cluster.{Broker, EndPoint}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metadata.BrokerRecord
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class BrokerRecordProcessor extends ApiMessageProcessor {
  override def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessage: ApiMessage): BrokerMetadataBasis = {
    apiMessage match {
      case brokerRecord: BrokerRecord =>
        val brokerMetadataValue = brokerMetadataBasis.getValue()

        val newMetadataCacheBasis: MetadataCacheBasis = process(List(brokerRecord), brokerMetadataValue.metadataCacheBasis)

        brokerMetadataBasis.newBasis(brokerMetadataValue.newValue(newMetadataCacheBasis))
      case unexpected => throw new IllegalArgumentException(s"apiMessage was not of type BrokerRecord: ${unexpected.getClass}")
    }
  }

  override def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessages: List[ApiMessage]): BrokerMetadataBasis = {
    if (apiMessages.isEmpty) {
      return brokerMetadataBasis
    }
    apiMessages.foreach(message => if (!message.isInstanceOf[BrokerRecord]) {
      throw new IllegalArgumentException(s"$getClass can only process a list of records when they are all broker records: $apiMessages")
    })
    val brokerMetadataValue = brokerMetadataBasis.getValue()
    val newMetadataCacheBasis: MetadataCacheBasis = process(
      apiMessages.asInstanceOf[List[BrokerRecord]], brokerMetadataValue.metadataCacheBasis)
    brokerMetadataBasis.newBasis(brokerMetadataValue.newValue(newMetadataCacheBasis))
  }

  // visible for testing
  private[server] def process(brokerRecords: List[BrokerRecord], metadataCacheBasis: MetadataCacheBasis) = {
    val metadataSnapshot = metadataCacheBasis.getValue()
    val existingAliveBrokers = metadataSnapshot.aliveBrokers
    val numAlreadyAlive = brokerRecords.count(broker => existingAliveBrokers.contains(broker.brokerId()))
    val newSize = existingAliveBrokers.size + brokerRecords.size - numAlreadyAlive
    // allocate new alive brokers/nodes
    val newAliveBrokers = new mutable.LongMap[Broker](newSize)
    val newAliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](newSize)
    // insert references to existing alive brokers/nodes for ones that don't correspond to the upserted broker
    for ((existingBrokerId, existingBroker) <- metadataSnapshot.aliveBrokers) {
      newAliveBrokers(existingBrokerId) = existingBroker
    }
    for ((existingBrokerId, existingListenerNameToNodeMap) <- metadataSnapshot.aliveNodes) {
      newAliveNodes(existingBrokerId) = existingListenerNameToNodeMap
    }
    // add new alive broker/nodes for the upserted brokers
    brokerRecords.foreach(brokerRecord => {
      val nodes = new util.HashMap[ListenerName, Node]
      val endPoints = new ArrayBuffer[EndPoint]
      val brokerId = brokerRecord.brokerId()
      brokerRecord.endPoints().forEach { ep =>
        val listenerName = new ListenerName(ep.name())
        endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
        nodes.put(listenerName, new Node(brokerId, ep.host, ep.port))
      }
      newAliveBrokers(brokerId) = Broker(brokerId, endPoints, Option(brokerRecord.rack))
      newAliveNodes(brokerId) = nodes.asScala
    })
    metadataCacheBasis.metadataCache.logListenersNotIdenticalIfNecessary(newAliveNodes)

    val newMetadataCacheBasis = metadataCacheBasis.newBasis(
      MetadataSnapshot(metadataSnapshot.partitionStates, metadataSnapshot.controllerId, newAliveBrokers, newAliveNodes))
    newMetadataCacheBasis
  }
}
