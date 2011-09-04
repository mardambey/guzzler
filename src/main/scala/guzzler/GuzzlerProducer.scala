package guzzler

import akka.actor.Actor
import akka.dispatch.Future
import net.lag.logging.Logger
import java.util.concurrent.BlockingQueue

/**
 * Takes lines from the blocking queue and
 * pushes them to consumers.
 */
class GuzzlerProducer(q:BlockingQueue[String]) extends Actor {

  val logger = Logger.get
  var read = true

  override def preStart() {
    val f = Future {
      while (read) {
        val sql = q.take()
        try {
          Util.processSql(sql)
        } catch {
          case e:Exception => logger.error(e, " [guzzler] Could not process SQL: " + sql)
          case ignore => logger.error(" [guzzler] Could not process SQL (unknown error): " + sql + " -> " + ignore)
        }
      }
    }
  }

  def receive = {
    case _ =>
  }
}