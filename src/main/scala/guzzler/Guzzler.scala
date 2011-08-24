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
import actors.Actor
import actors.Futures._
import org.apache.sshd._
import common.{NamedFactory, Factory}
import server.auth.UserAuthNone
import server.keyprovider.SimpleGeneratorHostKeyProvider
import java.io._
import collection.JavaConversions._
import server.{UserAuth, Environment, ExitCallback, Command}
import java.lang.StringBuffer
import scala.Int

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

  val sshd = new Sshd(2222, "/tmp/guzzler.ser")
  sshd.start()

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

class Subscribe(actor:Actor, event:String)

class Sshd(port:Int, hostKey:String) extends Actor {

  def act() {

    future {
      val sshd = SshServer.setUpDefaultServer()
      sshd.setPort(port);
      sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(hostKey))
      sshd.setShellFactory((new ShellFactory()).asInstanceOf[Factory[Command]])
      sshd.setUserAuthFactories(List(new UserAuthNone.Factory().asInstanceOf[NamedFactory[UserAuth]]))
      sshd.start()
    }

    loop {
      react {
        case _ =>
      }
    }
  }
}

class ShellFactory extends Factory[Shell] {
  override def create() : Shell = {
    new Shell()
  }
}

class Shell extends Command {

  var alive = true
  lazy val actor = new Actor() {

    val reader = new BufferedReader(new InputStreamReader(in.get))

    def readLine() : Option[String] = {

      try {
        Some(reader.readLine())
      } catch {
        case _ => None
      }
    }

    def act() {

      val reader = future {
        var reading = true
        while(reading) {
          readLine() match {
            case None => {
              reading = false
              exitCallback.get.onExit(0)
              exit()
            }
            case Some("exit") => {
              out.get.write(("See you!\r\n").getBytes)
              out.get.flush()
              out.get.close()
              exitCallback.get.onExit(0)
              exit()
            }
            case Some(cmd:String) => {
              out.get.write(("You sent: " + cmd + "\r\n").getBytes)
              out.get.flush()
            }
          }
        }
      }

      loop {
        react {
          case "Exit" => {
            out.get.close()
            reader()
            exit()
          }
        }
      }
    }
  }

  var in:Option[InputStream] = None
  var out:Option[OutputStream] = None
  var err:Option[OutputStream] = None
  var exitCallback:Option[ExitCallback] = None

  def destroy() {
    actor ! "Exit"
  }

  def start(env: Environment) {
    out.get.write("\r\n -=[ Welcome to Guzzler ]=-\r\n\r\n".getBytes)
    out.get.flush()
    actor.start()
  }

  def setExitCallback(callback: ExitCallback) { this.exitCallback = Some(callback) }

  def setErrorStream(err: OutputStream) { this.err = Some(err) }

  def setOutputStream(out: OutputStream) { this.out = Some(out) }

  def setInputStream(in: InputStream) { this.in = Some(in) }
}