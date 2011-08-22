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
import scala.Some


// connect to a rabbit mq host
case class Connect(rabbitHost: String)

// reconnect to a rabbit mq host
//case class Reconnect()

// disconnect from the rabbit mq host
case class Disconnect()

// create a channel and associate it with the given name
case class CreateChannel(channel:String)

// delcare a queue on a given channel
case class DeclareQueue(channel:String, queue:String)

// declare an exchange of the given topic on the given a channel
case class DeclareExchange(channel:String, exchange:String, exchangeType:String)

// create a channel and declare a given queue on it
case class CreateChannelWithQueue(channel:String, queue:String)

// create a channel and declare a given exchange on it
case class CreateChannelWithExchange(channel:String, exchangeName:String, exchangeType:String)

// send a message to the given channel and queue
case class SendQueue(obj: Array[Byte], channel:String, queue:String)

// send a message to the given channel and exchange
case class SendExchange(obj: Array[Byte], channel:String, exchange:String, routingKey:String)

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
  def createChannelWithExchange(channel:String, exchangeName:String, exchangeType:String) {
    val chan = createChannel(channel)
    declareExchange(channel, exchangeName, exchangeType)
  }

  /**
   * Creates a channel and declares a queue on
   * it given its name.
   */
  def createChannelWithQueue(channel:String, queue:String) {
    val chan = createChannel(channel)
    declareQueue(channel, queue)
  }

  /**
   * Creates a channel and associates it with the
   * given name.
   */
  def createChannel(name:String) : Option[Channel] = {
    val channel = Some(connection.get.createChannel())
    channel match {
      case Some(c) => {
        channels(name) = channel
        channel
      }
      case _ => None
    }
  }

  /**
   * Declares a queue by name on the given
   * channel. If the channel does not exist
   * it will be automatically created.
   */
  def declareQueue(channel:String, queue:String) {
    val chan = channels(channel).orElse(createChannel(channel))
    chan match {
      case None => logger.error("Could not create channel with queue " + channel + "::" + queue)
      case Some(c) => {
        // queue, durable, exclusive, auto-delete, args
        c.queueDeclare(queue, false, false, false, null)
      }
    }
  }

  /**
   * Declares an exchange by name on the given
   * channel. If the channel does not exist
   * it will be automatically created.
   */
  def declareExchange(channel:String, exchangeName:String, exchangeType:String) {
    val chan = channels(channel).orElse(createChannel(channel))
    chan match {
      case None => logger.error("Could not create channel with exchange " + channel + "::" + exchangeName + "(" + exchangeType + ")")
      case Some(c) => {
        c.exchangeDeclare(exchangeName, exchangeType)
      }
    }
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
        case Connect(host:String) => connect(host)
        case Disconnect() => disconnect()
        //case Reconnect() => reconnect()
        case CreateChannel(channel:String) => createChannel(channel)
        case DeclareQueue(channel:String, queue:String) => declareQueue(channel, queue)
        case DeclareExchange(channel:String, exchange:String, exchangeType:String) => declareExchange(channel, exchange, exchangeType)
        case CreateChannelWithQueue(channel:String, queue:String) => createChannelWithQueue(channel, queue)
        case CreateChannelWithExchange(channel:String, exchange:String, exchangeType:String) => createChannelWithExchange(channel, exchange, exchangeType)
        case SendQueue(obj, channel, queue) => {
          channels(channel) match {
            case None =>
            case Some(chan) => {
              try { chan.basicPublish("", queue, null, obj) }
              catch { case e: Exception => logger.error(e, "Exception caught while publishing to channel " + channel + " on queue " + queue) }
            }
          }
        }
        case SendExchange(obj, channel, exchange, routingKey) => {
          channels(channel) match {
            case None =>
            case Some(chan) => {
              try { chan.basicPublish(exchange, routingKey, null, obj) }
              catch { case e: Exception => logger.error(e, "Exception caught while publishing to channel " + channel + " on exchange " + exchange + " with key " + routingKey) }
            }
          }
        }

        case ignore =>
      }
    }
  }
}
