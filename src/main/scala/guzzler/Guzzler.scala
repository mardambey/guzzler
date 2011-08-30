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

import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import scala.sys.process.Process
import scala.sys.process.ProcessIO
import org.gibello.zql._
import net.lag.logging.Logger
import java.io._
import ssh.{SshdMessage, SshdSubscribe, Sshd}
import actors.{Futures, Actor}
import actors.Futures._
import collection.mutable.LinkedList
import java.lang.{Exception, ProcessBuilder, String, StringBuffer}

/**
 * TODO:
 * - add guaranteed exchangers and queues with message ACKs + durability
 * - add pause ability that will hold bin logs locally
 *   until un-paused
 */

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
  val sshd = new Sshd(Config.sshdPort.get, Config.sshdHostKeyPath.get)
  sshd.start()

  val sshdSubscriber = new Actor {
    def act() {
      loop {
        react {
          case SshdMessage(msg) => {
            msg.stripLineEnd match {
              case "guzzler pause" => {
                logger.info("Pausing main queue.")
                queue ! QueuePause()
              }
              case "guzzler resume" => {
                logger.info("Resuming main queue.")
                queue.restart()
              }
              case ignore =>
            }
          }
          case ignore =>
        }
      }
    }
  }
  sshdSubscriber.start()
  sshd ! SshdSubscribe(sshdSubscriber, "guzzler")

  // init queue
  private case class QueuePause()
  val queue = new Actor {
    def act() {
      loop {
        react {
          case QueuePause() => {
            exit()
          }
          case sql:String => {
            Util.processSql(sql)
          }
        }
      }
    }
  }
  queue.start()

  // start consumers
  Config.consumers.foreach(_.start())

  // find the current position of the binlog to stream
  val binLogPositionCmd = Config.mysqlCmd.get
  val binLogStdout = new StringBuffer()
  val binLogProcBuilder = {
    try {
      Some(Process(binLogPositionCmd, List("-P" + Config.mysqlPort.get, "-u" + Config.mysqlUser.get, "-p" + Config.mysqlPassword.get, "-h" + Config.mysqlHost.get, Config.mysqlDb.get, "-eshow master status")))
    } catch {
      case e:Exception => {
        logger.error(e, "Could not determine binlog position.")
        sys.exit(-1)
        None
      }
    }
  }

  var snapshotPosition:Long = 0
  val binLogIO = new ProcessIO(
    _ => (), // stdin not used
    stdout => scala.io.Source.fromInputStream(stdout).getLines().foreach(binLogStdout.append),
    stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(binLogStdout.append))

  val binLogProc = binLogProcBuilder.get.run(binLogIO)
  val binLogexitCode = binLogProc.exitValue()
  val binLogFilePosition = binLogStdout.toString.split("Binlog_Ignore_DB")(1).split("""\s+""").slice(0, 2)
  var stream = true

  while (stream) streamBinLog()

  def streamBinLog() {
    try {
      // build and run mysql binlog streamer, possibly resuming from a certain position
      val filePosition = if (snapshotPosition != 0) snapshotPosition else binLogFilePosition(1)

      logger.debug("Starting form binlong " + binLogFilePosition(0) + " at position " + filePosition)

      val binLogStreamCmd = Config.mysqlBinlogStreamer.get
      val binLogStreamProcBuilder = Process(binLogStreamCmd, List("-h" + Config.mysqlHost.get, "-P" + Config.mysqlPort.get, "-u" + Config.mysqlUser.get, "-p" + Config.mysqlPassword.get, "-R", "--start-position=" + filePosition,  "--stop-never", "--stop-never-slave-server-id=" + Config.mysqlSlaveServerId.get , "-f", binLogFilePosition(0)))

      val sqlRegex = """(?i)(insert |update |delete ).*""".r
      val atRegex = """(?i)# at (\d+)""".r

      val processBuilder = new java.lang.ProcessBuilder(
        List(binLogStreamCmd,
          "-h" + Config.mysqlHost.get,
          "-P" + Config.mysqlPort.get,
          "-u" + Config.mysqlUser.get,
          "-p" + Config.mysqlPassword.get,
          "-R", "--start-position=" + binLogFilePosition(1),
          "--stop-never",
          "--stop-never-slave-server-id=" + Config.mysqlSlaveServerId.get ,
          "-f", binLogFilePosition(0)).foldLeft(new java.util.LinkedList[String]())((list, w) => { list.add(w); list }))

      val process = processBuilder.start()
      val stdout = process.getInputStream

      val bufferedReader = new BufferedReader(new InputStreamReader(stdout))
      var line = readLine(bufferedReader)

      var iterate = true
      while (iterate) {

        line match {
          case None => {
            // timeout
            iterate = false
            process.destroy()
            throw new Exception("Caught timeout while reading from mysqlbinlog, restarting from " + snapshotPosition)
          }
          case Some(str) => {
            try {
              str match {
                case sqlRegex(sql) => {
                  queue ! str
                }
                case atRegex(position) => {
                  snapshotPosition = position.toLong
                }
                case ignore =>
              }
            } catch {
              case _ =>
            }
            line = readLine(bufferedReader)
          }
        }
      }
    } catch {
      case e:Exception => {
        logger.error(e, "Could not stream binary logs.")
      }
    }
  }

  def readLine(bufferedReader:BufferedReader) : Option[String] = {
    val f = future {
      Some(bufferedReader.readLine())
    }

    val results = Futures.awaitAll(2000, f).map(_ match {
      case s @ Some(str) => str
      case error => None
    }).asInstanceOf[List[Option[String]]]

    results.head
  }
}

case class Statement(s:ZStatement)

object Util {

  val logger = Logger.get
  val parser = new ZqlParser()

  def processSql(sql:String) {
    // FIXME: this is an ugly hack
    val scrubbedSql = sql.replaceAll("""\\'""", "") + ";"
    parser.initParser(new ByteArrayInputStream(scrubbedSql.getBytes))

    try {
      val statement = parser.readStatement()
      Config.consumers.par.foreach(_ ! Statement(statement))
    } catch {
      case e:Exception => logger.error(e, "Exception caught while parsing SQL '" + scrubbedSql)
    }
  }
}
