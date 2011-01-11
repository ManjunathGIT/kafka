/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.producer

import async.AsyncKafkaProducer
import org.I0Itec.zkclient.{ZkClient, IZkChildListener}
import kafka.utils.{StringSerializer, ZkUtils, Utils}
import kafka.serializer.Encoder

class RichProducer[T](config: RichProducerConfig,
                      partitioner: Partitioner[T],
                      serializer: Encoder[T]) {
  private val producer = new AsyncKafkaProducer[T](config)
  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                                      StringSerializer)
  // register a listener for /brokers/topics/rootTopic
  private val partitionsChangedListener = new ZKNewPartitionListener(config.rootTopic)
  zkClient.subscribeChildChanges(ZkUtils.brokerTopicsPath + "/" + config.rootTopic, partitionsChangedListener)
  
  def this(config: RichProducerConfig) {
    this(config, Utils.getObject(config.partitionerClass), Utils.getObject(config.serializerClass))
  }
  
  def send(data: T) {
    val subTopics = partitionsChangedListener.getSubTopics.toArray
    val partitionId = partitioner.partition(data, subTopics)
    if(partitionId < 0 || partitionId >= subTopics.length)
      throw new InvalidPartitionException("Invalid partition id : " + partitionId +
              "\n Valid values are in the range inclusive [0, " + (subTopics.length-1) + "]")

    val topic = subTopics(partitionId)
    val getTopic = (data:T) => topic
    producer.send(data, getTopic)
  }

  class ZKNewPartitionListener(val topic: String)
          extends IZkChildListener {
    // old list of sub-topics
    var oldSubTopics: Set[String] = Set() ++ ZkUtils.getSubTopics(zkClient, topic).toIterable
    
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      println("Current list of children from ZKPartitionerListener " + curChilds.toString)
      import scala.collection.JavaConversions._
      val updatedSubTopics = Set() ++ asIterable(curChilds)
      val newSubTopics: Set[String] = updatedSubTopics &~ oldSubTopics 
      println("New list of children from ZKPartitionerListener " + newSubTopics.toString)
      oldSubTopics = updatedSubTopics
    }

    def getSubTopics: Set[String] = oldSubTopics
  }
}