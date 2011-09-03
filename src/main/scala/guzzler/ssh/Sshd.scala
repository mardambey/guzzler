package guzzler.ssh

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

import akka.actor.{ActorRef, Actor}
import akka.actor.Actor._
import akka.dispatch.Future
import akka.util.duration._
import org.apache.sshd._
import common.{NamedFactory, Factory}
import server.auth.UserAuthNone
import server.keyprovider.SimpleGeneratorHostKeyProvider
import java.io._
import collection.JavaConversions._
import server.{UserAuth, Environment, ExitCallback, Command}
import java.lang.StringBuffer
import scala.Int
import java.util.concurrent.ConcurrentHashMap
import net.lag.logging.Logger

case class SshdSubscribe(actor:ActorRef, event:String)
case class SshdMessage(msg:String)

/**
 * Creates an ssh server on the given port using the given
 * path to (possibly, if it exists) load the host's key.
 * Actors can then subscribe to this server providing a
 * string that is matched against incoming messages and
 * routed over to them as a message if the match succeeds.
 */
class Sshd(port:Int, hostKeyPath:String) extends Actor {

  val subscribers = new ConcurrentHashMap[String, ActorRef]()

  override def preStart() {

    Future {
      // set up and launch the ssh server
      val sshd = SshServer.setUpDefaultServer()
      sshd.setPort(port);
      sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(hostKeyPath))
      sshd.setShellFactory((new ShellFactory(self)).asInstanceOf[Factory[Command]])
      sshd.setUserAuthFactories(List(new UserAuthNone.Factory().asInstanceOf[NamedFactory[UserAuth]]))
      sshd.start()
    }
  }

  def receive = {
    case SshdSubscribe(actor, event) => {
      subscribers(event) = actor
    }
    case m @ SshdMessage(msg) => {
      subscribers.par.foreach(s => if (msg.startsWith(s._1)) s._2 ! m)
    }
  }
}

class ShellFactory(sshd:ActorRef) extends Factory[Shell] {
  override def create() : Shell = {
    new Shell(sshd)
  }
}

class Shell(sshd:ActorRef) extends Command {

  var in:Option[InputStream] = None
  var out:Option[OutputStream] = None
  var err:Option[OutputStream] = None
  var exitCallback:Option[ExitCallback] = None
  var alive = true
  var actor:ActorRef = _

  def destroy() {
  }

  def start(env: Environment) {
    out.get.write("\r\n -=[ Welcome to Guzzler ]=-\r\n\r\n".getBytes)
    out.get.flush()
    actor = actorOf(new ShellActor(sshd, this, in.get, out.get)).start()
  }

  def setExitCallback(callback: ExitCallback) { this.exitCallback = Some(callback) }

  def setErrorStream(err: OutputStream) { this.err = Some(err) }

  def setOutputStream(out: OutputStream) { this.out = Some(out) }

  def setInputStream(in: InputStream) { this.in = Some(in) }
}

class ShellActor(sshd:ActorRef, shell:Shell, in:InputStream, out:OutputStream) extends Actor {

  val logger = Logger.get
  var reader:Future[Unit] = _
  var reading = true
  val lineReader = new BufferedReader(new InputStreamReader(in))

  def readLine() : Option[String] = {
    try {
      Some(lineReader.readLine())
    } catch {
      case _ => None
    }
  }

  override def preStart() {
    reader = Future {
      while(reading) {
        val strBuf = new StringBuffer()
        Stream.continually(in.read).takeWhile(_ != -1).foreach(i => {
          i match {
            case 13 => { // enter
              out.write(("\r\n").getBytes)
              out.flush()
              sshd ! SshdMessage(strBuf.toString)
              strBuf.delete(0, strBuf.length())
              strBuf
            }
            case 4 => {
              strBuf.delete(0, strBuf.length())
              out.write(("\r\n\r\n*guzzle* *guzzle*!\r\n\r\n").getBytes)
              out.flush()
              out.close()
              reading = false
              shell.exitCallback.get.onExit(0)
              self ! "Exit"
              strBuf
            }
            case c => {
              strBuf.append(c.asInstanceOf[Char])
              out.write(c.asInstanceOf[Char])
              out.flush()
              strBuf
            }
          }
        })
      }
    }
  }

  def receive = {
    case "Exit" => {
      try { reader.await(2.second) } catch { case e:Exception => logger.error(e, " [guzzler] Timed out while waiting for ssh connection to close.")}
      self.exit()
    }
  }
}
