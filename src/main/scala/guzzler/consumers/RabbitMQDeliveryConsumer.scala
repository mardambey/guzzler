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

package guzzler.consumers

import akka.actor.Actor
import akka.actor.Actor._
import org.gibello.zql._
import guzzler.rabbitmq._
import java.lang.Exception
import net.lag.logging.Logger
import net.lag.configgy.ConfigMap
import guzzler._
import ssh.{SshdMessage, SshdSubscribe}

/**
 * Accepts bin log messages and pushes them into
 * RabbitMQ. Messages are associated with routing
 * keys that can be filtered by consumers. Keys
 * are built in the form "db.table.op" where db
 * and table are the database and corresponding
 * table, and op is one of insert, update, and
 * delete.
 */
class RabbitMQDeliveryConsumer extends Actor {

  val logger = Logger.get
  var DATABASE:String = _
  var DATABASE_EXCHANGE:String = _
  val rabbitMQ = actorOf[RabbitMQ]

  val config:ConfigMap = {
    try {
      val conf = Config.config.getConfigMap("rabbitMQDeliveryConsumer").get
      DATABASE = conf.getString("database").get
      DATABASE_EXCHANGE = DATABASE + "_" + conf.getString("exchange").get
      rabbitMQ.start()
      rabbitMQ ! Connect(conf.getString("host").get)
      rabbitMQ ! CreateChannelWithExchange(DATABASE, DATABASE_EXCHANGE, "topic")
      conf
    } catch {
      case _ => {
        logger.error("RabbitMQDeliveryConsumer: Could not load configuration.")
        throw new Exception("RabbitMQDeliveryConsumer: Could not load configuration.")

      }
    }
  }

  val INSERT = "insert"
  val UPDATE = "update"
  val DELETE = "delete"

  def getKey(table:String, op:String) : String = DATABASE + "." + table + "." + op

  def beamOff(data:Array[Byte], table:String, op:String) {
    rabbitMQ ! SendExchange(data, DATABASE, DATABASE_EXCHANGE, getKey(table, op))
  }

  // register with the sshd server to receive commands
  override def preStart() { Guzzler.sshd ! SshdSubscribe(self, "rabbitmq") }

  def receive = {
    case SshdMessage(msg) => {
      logger.info("RabbitMQDeliveryConsumer: Got an ssh message: " + msg)
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