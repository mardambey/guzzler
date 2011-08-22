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
import java.lang.StringBuffer
import org.gibello.zql._
import java.io.ByteArrayInputStream
import net.lag.logging.Logger

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
  Config.load("guzzler.conf")

  val binLogPositionCmd = Config.mysqlCmd.get
  val binLogStdout = new StringBuffer()
  val binLogProcBuilder = Process(binLogPositionCmd, List("-u" + Config.mysqlUser.get, "-p" + Config.mysqlPassword.get, "-h" + Config.mysqlHost.get, Config.mysqlDb.get, "-eshow master status"))

  val binLogIO = new ProcessIO(
    _ => (), // stdin not used
    stdout => scala.io.Source.fromInputStream(stdout).getLines().foreach(binLogStdout.append),
    stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(binLogStdout.append))

  val binLogProc = binLogProcBuilder.run(binLogIO)
  val binLogexitCode = binLogProc.exitValue()
  val binLogFilePosition = binLogStdout.toString.split("Binlog_Ignore_DB")(1).split("""\s+""").slice(0, 2)

  val binLogStreamCmd = Config.mysqlBinlogStreamer.get
  val binLogStreamProcBuilder = Process(binLogStreamCmd, List("-h" + Config.mysqlHost.get, "-P3306", "-u" + Config.mysqlUser.get, "-p" + Config.mysqlPassword.get, "-R", "--start-position=" + binLogFilePosition(1),  "--stop-never" , "-f", binLogFilePosition(0)))

  val sqlRegex = """(?i)(insert|update|delete) .*""".r

  val binLogStreamIO = new ProcessIO(
    _ => (), // stdin not used
    stdout => scala.io.Source.fromInputStream(stdout).getLines().foreach(line => {
      try {
        line match {
          case line @ sqlRegex(sql) => {
            Util.processSql(line)
          }
          case _ =>
        }
      } catch {
        case _ =>
      }
    }),
    // FIXME: either remove this or do something useful with it
    stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(println))

  val binLogStreamProc = binLogStreamProcBuilder.run(binLogStreamIO)
  val binLogStreamExitCode = binLogStreamProc.exitValue()
}

case class Statement(s:ZStatement)

object Util {

  val logger = Logger.get

  def processSql(sql:String) {
    val parser = new ZqlParser()
    // FIXME: this is an ugly hack
    val scrubbedSql = sql.replaceAll("""\\'""", "") + ";"
    parser.initParser(new ByteArrayInputStream(scrubbedSql.getBytes))

    try {
      val statement = parser.readStatement()
      Config.consumers.foreach(_ ! Statement(statement))
    } catch {
      case e:Exception => logger.error(e, "Exception caught while parsing SQL '" + scrubbedSql)
    }
  }
}
