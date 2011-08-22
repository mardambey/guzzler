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

import actors.Actor
import org.gibello.zql._
import guzzler.rabbitmq._
import guzzler.Statement

class RabbitMQDeliveryConsumer extends Actor {

  val DATABASE = "messaging"
  val DATABASE_EXCHANGE = DATABASE + "_binlogs"
  val INSERT = "insert"
  val UPDATE = "update"
  val DELETE = "delete"
  val rabbitMQ = new RabbitMQ
  rabbitMQ.start()
  rabbitMQ ! Connect("localhost")
  rabbitMQ ! CreateChannelWithExchange(DATABASE, DATABASE_EXCHANGE, "topic")

  def getKey(table:String, op:String) : String = DATABASE + "." + table + "." + op

  def beamOff(data:Array[Byte], table:String, op:String) { rabbitMQ ! SendExchange(data, DATABASE, DATABASE_EXCHANGE, getKey(table, op)) }

  def act() {
    loop {
      react {
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