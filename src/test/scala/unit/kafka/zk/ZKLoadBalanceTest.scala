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

package kafka.zk

import junit.framework.TestCase
import junit.framework.Assert._
import kafka.utils._
import java.util.Collections
import kafka.cluster.{Broker, Cluster}
import kafka.consumer.{Fetcher, ConsumerConfig, ZookeeperConsumerConnector}
import java.lang.Thread
import kafka.{TestZKUtils, TestUtils}
import org.junit.Test

class ZKLoadBalanceTest extends TestCase with ZooKeeperTestHarness {
  val zkConnect = TestZKUtils.zookeeperConnect
  var dirs : ZKGroupTopicDirs = null
  val topic = "topic1"
  val group = "group1"
  val firstConsumer = "consumer1"
  val secondConsumer = "consumer2"

  override def setUp() {
    super.setUp()

    dirs = new ZKGroupTopicDirs(group, topic)
  }

  @Test
  def testLoadBalance() {
    // create the first partition
    ZkUtils.setupPartition(zookeeper.client, 400, "broker1", 1111, "topic1", 2)
    // add the first consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, firstConsumer))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, false)
    zkConsumerConnector1.createMessageStreams(Map(topic -> 1))

    {
      // check Partition Owner Registry
      val consumerDirs = new ZKGroupTopicDirs(group, topic + "/400")
      val actual_1 = getZKChildrenValues(consumerDirs.consumerOwnerDir)
      val expected_1 = List( ("400-0", "group1_consumer1-0"), ("400-1", "group1_consumer1-0") )

      checkSetEqual(actual_1, expected_1)
    }

    // add a second consumer
    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, secondConsumer))
    val ZKConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, false)
    ZKConsumerConnector2.createMessageStreams(Map(topic -> 1))
    // wait a bit to make sure rebalancing logic is triggered
    Thread.sleep(200)

    {
      // check Partition Owner Registry
      val consumerDirs = new ZKGroupTopicDirs(group, topic + "/400")
      val actual_2 = getZKChildrenValues(consumerDirs.consumerOwnerDir)
      val expected_2 = List( ("400-0", "group1_consumer1-0"), ("400-1", "group1_consumer2-0") )
      checkSetEqual(actual_2, expected_2)
    }

    {
      // add a few more partitions
      val brokers = List(
        (200, "broker2", 1111, "topic1", 2),
        (300, "broker3", 1111, "topic1", 2))

      for ((brokerID, host, port, topic, nParts) <- brokers)
        ZkUtils.setupPartition(zookeeper.client, brokerID, host, port, topic, nParts)

      val allBrokers = List(
        (400, "broker1", 1111, "topic1", 2),
        (200, "broker2", 1111, "topic1", 2),
        (300, "broker3", 1111, "topic1", 2))

      // wait a bit to make sure rebalancing logic is triggered
      Thread.sleep(200)
      var allConsumerDirs = allBrokers.map(b => new ZKGroupTopicDirs(group, topic + "/" + b._1))

      // check Partition Owner Registry
      var actual_3: List[(String, String)] = List()
      allConsumerDirs.foreach(d => actual_3 = actual_3 ++ getZKChildrenValues(d.consumerOwnerDir))
      val expected_3 = List( ("200-0", "group1_consumer1-0"),
                             ("200-1", "group1_consumer2-0"),
                             ("300-0", "group1_consumer1-0"),
                             ("300-1", "group1_consumer2-0"),
                             ("400-0", "group1_consumer1-0"),
                             ("400-1", "group1_consumer2-0"))
      checkSetEqual(actual_3, expected_3)
    }

    {
      // now delete a partition
      ZkUtils.deletePartition(zookeeper.client, 400, "topic1")

      val allBrokers = List(
        (400, "broker1", 1111, "topic1", 1),
        (200, "broker2", 1111, "topic1", 2),
        (300, "broker3", 1111, "topic1", 2))

      // wait a bit to make sure rebalancing logic is triggered
      Thread.sleep(500)
      val allConsumerDirs = allBrokers.map(b => new ZKGroupTopicDirs(group, topic + "/" + b._1))

      var actual_4: List[(String, String)] = List()
      allConsumerDirs.foreach(d => actual_4 = actual_4 ++ getZKChildrenValues(d.consumerOwnerDir))

      // check Partition Owner Registry
      val expected_4 = List( ("200-0", "group1_consumer1-0"),
                             ("200-1", "group1_consumer2-0"),
                             ("300-0", "group1_consumer1-0"),
                             ("300-1", "group1_consumer2-0") )
      checkSetEqual(actual_4, expected_4)
    }

    zkConsumerConnector1.shutdown
    ZKConsumerConnector2.shutdown
  }

  private def getZKChildrenValues(path : String) : Seq[Tuple2[String,String]] = {
    import scala.collection.JavaConversions._
    val children = zookeeper.client.getChildren(path)
    Collections.sort(children)
    val childrenAsSeq : Seq[java.lang.String] = asBuffer(children)
    childrenAsSeq.map(partition =>
      (partition, zookeeper.client.readData(path + "/" + partition).asInstanceOf[String]))
  }

  private def checkSetEqual(actual : Seq[Tuple2[String,String]], expected : Seq[Tuple2[String,String]]) {
    assertEquals(expected.length, actual.length)
    val actualSet = actual.toSet
    val expectedSet = expected.toSet
    val diffSet = actualSet &~ expectedSet
    println("Expected set: " + expected.toString)
    println("Actual set: " + actual.toString)
    assertEquals("Difference of sets (expected, actual): (" + expected.toString + "," +
            actual.toString + ") should be 0", 0, diffSet.size)
  }
}