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

package guzzler

import net.lag.logging.Logger
import ssh.{SshdSubscribe, Sshd}
import akka.actor.Actor._
import net.sf.jsqlparser.parser.CCJSqlParserManager
import java.io.StringReader
import net.sf.jsqlparser.statement.Statement



/**
 * Guzzler - streams binary logs from a remote MySQL
 * server and parses statements handing them off
 * to consumers (Actors) for asynchronous processing.
 */
object Guzzler extends App {

  // TODO: get this from the env too
  // load configuration
  Config.load("guzzler.conf")

  // logger object
  val logger = Logger.get

  // start sshd
  val sshd = actorOf(new Sshd(Config.sshdPort.get, Config.sshdHostKeyPath.get)).start()

  // start consumers
  Config.consumers.foreach(_.start())

  // subscribe to the ssh server
  val sshdSubscriber = actorOf[GuzzlerSshdSubscriber].start()
  sshd ! SshdSubscribe(sshdSubscriber, "guzzler")

  // queue that pushes out messages to consumers
  val queue = new java.util.concurrent.ArrayBlockingQueue[String](1)

  val streamer = actorOf(new GuzzlerBinlogStreamer(queue)).start()

  val producer = actorOf(new GuzzlerProducer(queue)).start()

  def getStreamer = streamer
}

object Util {

  val logger = Logger.get
  val parser = new CCJSqlParserManager()

  def processSql(sql:String) {
    // FIXME: this is an ugly hack
    val scrubbedSql = sql.replaceAll("""\\'""", "") + ";"

    try {
      val statement = parser.parse(new StringReader(scrubbedSql))
      Config.consumers.par.foreach(_ ! SqlStatement(statement, sql))
    } catch {
      case e:Exception => logger.error(e, " [guzzler] Exception caught while parsing SQL '" + scrubbedSql)
      case ignore => logger.error(" [guzzler] Could not process SQL (unknown error): " + scrubbedSql + " -> " + ignore)
    }
  }
}

// wraps a Statement
case class SqlStatement(statement:Statement, sql:String)

// pauses the queue, consumers stop getting messages
case class QueuePause()

// resumes the queue, consumers get messages again
case class QueueResume()
