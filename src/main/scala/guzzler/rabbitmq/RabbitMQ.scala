/*
 * Guzzler - Stream MySQL binary logs and act on them
 * Copyright (C) 2011 Hisham Mardam-Bey <hisham.mardambey@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package guzzler.rabbitmq

import actors.Actor
import net.lag.logging.Logger
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import java.util.concurrent.{ConcurrentHashMap => JCMap}
import scala.collection.JavaConversions._


case class Connect(rabbitHost: String)
//case class Reconnect()
case class Disconnect()
case class CreateChannelWithQueue(queue:String)
case class CreateChannelWithExchange(exchangeName:String, exchangeType:String)
case class SendQueue(obj: Array[Byte], queue:String)
case class SendExchange(obj: Array[Byte], exchange:String, routingKey:String)

/**
 * TODO:
 * - add message acknowledgements to rabbitmq
 * - add fanout and guaranteed exhangers and queues
 * - add pause ability that will hold bin logs locally
 *   until unpaused
 */

/**
 * Handles all the interaction with the RabbitMQ server.
 */
class RabbitMQ extends Actor {

  val logger = Logger.get(getClass.getName)

  var rabbitHost:Option[String] = None
  var channels = new JCMap[String, Option[Channel]]()
  var connection:Option[Connection] = None

  /**
   * Connects to a RabbitMQ server at the given host.
   */
  def connect(rabbitHost: String) : Boolean = {
    this.rabbitHost = Some(rabbitHost)
    _connect()
    true
  }

  /**
   * Creates a channel and declares an exchange on it
   * given its name and type.
   */
  def createChannelWithExchange(exchangeName:String, exchangeType:String) {
    val channel = Some(connection.get.createChannel())
    channel.get.exchangeDeclare(exchangeName, exchangeType)
    channels(exchangeName) = channel
  }

  /**
   * Creates a channel and declares a queue on
   * it given its name.
   */
  def createChannelWithQueue(c:String) {
    val channel = Some(connection.get.createChannel())
    channel.get.queueDeclare(c, false, false, false, null)
    channels(c) = channel
  }

  //def recreateChannels() { channels.foreach(c => { createChannelWithQueue(c._1) }) }

  //def reconnect() : Boolean = {
  //  val ret = _connect()
  //  recreateChannels()
  //  ret
  //}

  // TODO: report errors and implement reconnect logic
  def _connect() : Boolean = {
    try {
      logger.debug("Connecting to RabbitMQ on " + rabbitHost.get)
      val factory = new ConnectionFactory()
      factory.setHost(rabbitHost.get)
      connection = Some(factory.newConnection())
      true
    } catch {
      case e:Exception => logger.error(e, "Exception caught while connecting.")
    }

    false
  }

  def disconnect() {
    channels.foreach(c => {
      try {
        c._2.get.close()
        channels(c._1) = None
      }
    })

    try {
      connection.get.close()
      connection = None
    } catch {
      case e:Exception => logger.error(e, "Exception caught while closing connection.")
    }
  }

  def act() {
    loop {
      react {
        case CreateChannelWithQueue(queue:String) => createChannelWithQueue(queue)
        case CreateChannelWithExchange(exchange:String, exchangeType:String) => createChannelWithExchange(exchange, exchangeType)
        case Connect(host:String) => connect(host)
        case Disconnect() => disconnect()
        //case Reconnect() => reconnect()
        case SendQueue(obj, queue) => {
          channels(queue) match {
            case None =>
            case Some(channel) => {
              try { channel.basicPublish("", queue, null, obj) }
              catch { case e: Exception => logger.error(e, "Exception caught while publishing.") }
            }
          }
        }
        case SendExchange(obj, exchange, routingKey) => {
          channels(exchange) match {
            case None =>
            case Some(channel) => {
              try { channel.basicPublish(exchange, routingKey, null, obj) }
              catch { case e: Exception => logger.error(e, "Exception caught while publishing.") }
            }
          }
        }

        case ignore =>
      }
    }
  }
}
