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

import scala.sys.process.Process
import scala.sys.process.ProcessIO
import org.gibello.zql._
import net.lag.logging.Logger
import ssh.{SshdMessage, SshdSubscribe, Sshd}
import actors.{Futures, Actor}
import actors.Futures._
import java.io.{ByteArrayInputStream, InputStreamReader, BufferedReader}

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
              case "guzzler state" => {
                logger.info(" [guzzler] Current queue state is " + queue.getState)
              }
              case "guzzler stream restart" => {
                logger.info(" [guzzler] Received request to restart binlog streaming.")
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

  // pauses the queue, consumers stop getting messages
  private case class QueuePause()
  // shows the state of the queue
  private case class QueueState()
  // restarts the binlog streaming process
  private case class StreamRestart()

  // holds the current binlog snapshot
  var snapshotPosition:Long = 0

  // holds messages on their way out to consumers
  val queue = createMessageQueue()
  queue.start()

  // start consumers
  Config.consumers.foreach(_.start())

  // find the current name and position of the binlog to stream
  var binLogFilePosition = getBinLogCurrentPosition.getOrElse({die("Could not get current binlog position.")}).asInstanceOf[Array[String]]
  var stream = true

  // if set to true, the first record will be
  // skipped using --offset=1 in mysqlbinlog
  // this is used when theres a recovery from
  // a crash or a restart of the streamer
  var skipRecord = false

  // stream binlogs until interrupted
  while (stream) {
    // if we're here then we either need to
    // restart or we need to exit
    streamBinLog(binLogFilePosition, skipRecord) match {
      case true => {

        var keepTrying = true
        var results:Option[List[Array[String]]] = None

        for (i <- 1 to Config.maxReconnectAttempts.get if keepTrying) {
          logger.info(" [guzzler] Determining location to stream from (attempt " + i + " or " + Config.maxReconnectAttempts.get + ")")
          val f = future { getBinLogCurrentPosition.get }
          results = Some(Futures.awaitAll(2000, f).map(_ match {
              case s @ Some(arr:Array[String]) => { keepTrying = false; arr }
              case error => None
          }).asInstanceOf[List[Array[String]]])
        }

        results match {
          case None => die("Could not determine the current binary log position, last good position is " + binLogFilePosition(0) + " -> " + binLogFilePosition(1))
          case _ =>
        }

        val curBinLogPosition = results.get(0)

        binLogFilePosition(0) = curBinLogPosition(0)
        binLogFilePosition(1) = snapshotPosition.toString
        logger.info(" [guzzler] Resuming from binlog file " + binLogFilePosition(0) +
          " from position " + snapshotPosition +
          " (current server position: " + curBinLogPosition(1) + ")")

        // grace period
        Thread.sleep(2000)
      }
      case false => {
        stream = false
        die(" [guzzler] Not resuming, exiting.")
      }
    }

    // being here means we either
    // crashed or were restarted,
    // in both cases skip the first
    // record that is received
    skipRecord = true
  }

  def createMessageQueue() : Actor = new Actor {
    def act() {
      loop {
        react {
          case QueuePause() => {
            exit()
          }
          case sql:String => {
            try {
              Util.processSql(sql)
            } catch {
              case e:Exception => logger.error(e, "Could not process SQL: " + sql)
              case ignore => logger.error("Could not process SQL (unknown error): " + sql + " -> " + ignore)
            }
          }
          case _ =>
        }
      }
    }
  }

  def streamBinLog(binLogFilePosition:Array[String], skipRecord:Boolean = false) : Boolean = {
    try {
      // build and run mysql binlog streamer, possibly resuming from a certain position
      logger.info(" [guzzler] Starting form binlong " + binLogFilePosition(0) + " at position " + binLogFilePosition(1))

      val binLogStreamCmd = Config.mysqlBinlogStreamer.get
      val args = List(
        "-h" + Config.mysqlHost.get,
        "-P" + Config.mysqlPort.get,
        "-u" + Config.mysqlUser.get,
        "-p" + Config.mysqlPassword.get,
        "-R", "--start-position=" + binLogFilePosition(1),
        "--stop-never",
        "--stop-never-slave-server-id=" + Config.mysqlSlaveServerId.get ,
        "-f", binLogFilePosition(0)) ++ (skipRecord match {
        case true => List("--offset=1")
        case _ => List[String]()})

      if (skipRecord) logger.info(" [guzzler] Skipping the first record in the binlog.")

      val binLogStreamProcBuilder = Process(binLogStreamCmd, args)

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

      logger.info(" [guzzler] Guzzling down binary logs *guzzle* *guzzle*")

      while (iterate) {
        line match {
          case None => {
            // timeout
            iterate = false
            process.destroy()
            throw new Exception(" [guzzler] Caught timeout while reading from mysqlbinlog at position " + snapshotPosition)
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
      false
    } catch {
      case e:Exception => {
        logger.error(e, " [guzzler] Could not stream binary logs.")
        true
      }
    }
  }

  def die(msg:String, e:Option[Exception] = None) {
    e match {
      case Some(e) => logger.error(e, msg)
      case any => logger.error(msg)
    }

    sys.exit(-1)
  }

  def getBinLogCurrentPosition : Option[Array[String]] = {
    val binLogPositionCmd = Config.mysqlCmd.get
    val binLogStdout = new StringBuffer()
    val binLogProcBuilder = {
      try {
        Some(Process(binLogPositionCmd, List("-P" + Config.mysqlPort.get, "-u" + Config.mysqlUser.get, "-p" + Config.mysqlPassword.get, "-h" + Config.mysqlHost.get, Config.mysqlDb.get, "-eshow master status")))
      } catch {
        case e:Exception => {
          logger.error(e, " [guzzler] Could not determine binlog position.")
          None
        }
      }
    }

    binLogProcBuilder match {
      case Some(procBuilder) => {
        val binLogIO = new ProcessIO(
          _ => (), // stdin not used
          stdout => scala.io.Source.fromInputStream(stdout).getLines().foreach(binLogStdout.append),
          stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(binLogStdout.append))

        val binLogProc = binLogProcBuilder.get.run(binLogIO)
        val binLogexitCode = binLogProc.exitValue()
        val binLogFilePosition = binLogStdout.toString.split("Binlog_Ignore_DB")(1).split("""\s+""").slice(0, 2)
        Some(binLogFilePosition)
      }
      case error => {
        None
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
      case e:Exception => logger.error(e, " [guzzler] Exception caught while parsing SQL '" + scrubbedSql)
      case ignore => logger.error(" [guzzler] Could not process SQL (unknown error): " + scrubbedSql + " -> " + ignore)
    }
  }
}
