package guzzler.consumers

import guzzler.rabbitmq.{Send, AddChannel, Connect, RabbitMQ}
import org.gibello.zql._
import actors.Actor

case class Statement(s:ZStatement)

class UnreadMessageCountConsumer extends Actor {
  def act() {
    loop {
      react {
        case Statement(s) => {
          s match {
            case q:ZInsert => {
              println("Got an insert: " + q.getColumns + " -> " + q.getValues)
            }
            case q:ZUpdate => {
              println("Got an update: " + q.getWhere)
            }
            case q:ZDelete => {
              println("Got a delete: " + q.getWhere)
            }
          }
        }
      }
    }
  }
}

class RabbitMQDeliveryConsumer extends Actor {

  val channel = "messaging"
  val rabbitMQ = new RabbitMQ
  rabbitMQ.start()
  rabbitMQ ! Connect("localhost")
  rabbitMQ ! AddChannel(channel)

  def act() {
    loop {
      react {
        case Statement(s) => {
          s match {
            case q:ZInsert => {
              println("Got an insert: " + q.getColumns + " -> " + q.getValues)
              rabbitMQ ! Send(q.toString.getBytes, channel)
            }
            case q:ZUpdate => {
              println("Got an update: " + q.getWhere)
            }
            case q:ZDelete => {
              println("Got a delete: " + q.getWhere)
            }
          }
        }
      }
    }
  }
}