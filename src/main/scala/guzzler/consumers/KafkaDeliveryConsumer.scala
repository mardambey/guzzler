package guzzler.consumers

import actors.Actor
import actors.Futures._
import guzzler.ssh.{SshdMessage, SshdSubscribe}
import guzzler.{Statement, Guzzler}
import org.gibello.zql.{ZDelete, ZUpdate, ZInsert}
import kafka.producer.{ProducerData, Producer, ProducerConfig}
import net.lag.logging.Logger
import kafka.consumer.{Consumer, ConsumerConfig}
import java.util.{HashMap, Properties}
import scala.collection.JavaConversions._
import kafka.utils.Utils

class KafkaDeliveryConsumer extends Actor {

  val INSERT = "insert"
  val UPDATE = "update"
  val DELETE = "delete"

  val logger = Logger.get

  val props = new Properties()

  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("zk.connect", "localhost:2181")

  // Use random partitioner. Don't need the key type. Just set it to Integer.
  // The message is of type String.
  val producer = new Producer[Int, String](new ProducerConfig(props))

  def beamOff(data:Array[Byte], table:String, op:String) {
    producer.send(new ProducerData[Int, String]("guzzler", new String(data))) //table + "." + op, new String(data)))
  }

  val consumer = new KafkaConsumer
  consumer.start()

  def act() {

      // register with the sshd server to receive commands
      Guzzler.sshd ! SshdSubscribe(this, "kafka")

      loop {
        react {
          case SshdMessage(msg) => {
            logger.info("KafkaDeliveryConsumer: Got an ssh message: " + msg)
          }
          case Statement(s) => {
            s match {
              case q:ZInsert => {
                beamOff(q.toString.getBytes, q.getTable, INSERT)
              }
              case q:ZUpdate => {
                beamOff(q.toString.getBytes, q.getTable, UPDATE)
              }
              case q:ZDelete => {
                beamOff(q.toString.getBytes, q.getTable, DELETE)
              }
            }
          }
        }
      }
    }
}

class KafkaConsumer extends Actor {

  val logger = Logger.get

  val props = new Properties()

  props.put("zk.connect", "127.0.0.1:2181")
  props.put("groupid", "guzzler-consumers")
  props.put("zk.sessiontimeout.ms", "400")
  props.put("zk.synctime.ms", "200")
  props.put("autocommit.interval.ms", "1000")

  val consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
  val topics = new HashMap[String, java.lang.Integer]()
  topics.put("guzzler", 1)
  val topicMessageStreams = consumerConnector.createMessageStreams(topics);
  val streams = topicMessageStreams.get("guzzler");

  streams.foreach(s => {
    future {
      s.foreach(m => {
        //println("Consumed message: " + Utils.toString(m.payload, "UTF-8"))
      })
    }
  })


  def act() {

      // register with the sshd server to receive commands
      //Guzzler.sshd ! SshdSubscribe(this, "kafka-consumer")

      loop {
        react {
          case SshdMessage(msg) => {
            logger.info("KafkaDeliveryConsumer: Got an ssh message: " + msg)
          }
          case "consume" => {

          }
        }
      }
    }
}