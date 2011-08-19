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
case class AddChannel(rabbitQueue:String)
case class Reconnect()
case class Disconnect()
case class Send(obj: Array[Byte], channel:String)

/**
 * Handles all the interaction with the RabbitMQ server.
 */
class RabbitMQ extends Actor {

  val logger = Logger.get(getClass.getName)

  var rabbitHost:Option[String] = None
  var channels = new JCMap[String, Option[Channel]]()
  var connection:Option[Connection] = None

  def connect(rabbitHost: String) : Boolean = {
    this.rabbitHost = Some(rabbitHost)
    _connect()
    true
  }

  def addChannel(c:String) {
    val channel = Some(connection.get.createChannel())
    channels(c) = channel
    channel.get.queueDeclare(c, false, false, false, null)
  }

  def recreateChannels() { channels.foreach(c => { addChannel(c._1) }) }

  def reconnect() : Boolean = {
    val ret = _connect()
    recreateChannels()
    ret
  }

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
        case AddChannel(channel:String) => addChannel(channel)
        case Connect(host:String) => connect(host)
        case Disconnect() => disconnect()
        case Reconnect() => reconnect()
        case Send(obj, chan) => {
          channels(chan) match {
            case None =>
            case Some(channel) => {
              try { channel.basicPublish("", chan, null, obj) }
              catch { case e: Exception => logger.error(e, "Exception caught while publishing.") }
            }
          }
        }

        case ignore =>
      }
    }
  }
}
