package guzzler

import net.lag.logging.Logger
import ssh.{SshdMessage}
import akka.actor.Actor

/**
 * Registers a Guzzler's core commands with the
 * ssh interface for remote control.
 */
class GuzzlerSshdSubscriber extends Actor {

  // logger object
  val logger = Logger.get

  // regex used to seek to the given file and position
  val streamSeek = "guzzler stream seek (.*?) (.*?)$".r

  def receive = {
    case SshdMessage(msg) => {
      msg.stripLineEnd match {
        case "guzzler queue pause" => {
          logger.info(" [guzzler] Pausing main queue.")
          Guzzler.getStreamer ! QueuePause()
        }
        case "guzzler queue resume" => {
          logger.info(" [guzzler] Resuming main queue.")
          Guzzler.getStreamer ! QueueResume()
        }
        //case "guzzler state" => {
        //  logger.info(" [guzzler] Current queue state is " + queue.getState)
        //}
        case streamSeek(file:String, position:String) => {
          logger.info(" [guzzler] Received request to seek binlog " + file + " and " + position + " and stream.")
          Guzzler.getStreamer ! StreamSeek(file, position.toLong)
        }
        case "guzzler stream start" => {
          logger.info(" [guzzler] Received request to start binlog streaming.")
          Guzzler.getStreamer ! StreamStart()
        }
        case "guzzler stream stop" => {
          logger.info(" [guzzler] Received request to stop binlog streaming.")
          Guzzler.getStreamer ! StreamStop()
        }
        case "guzzler stream restart" => {
          logger.info(" [guzzler] Received request to restart binlog streaming.")
          Guzzler.getStreamer ! StreamRestart()
        }

        case unknown => logger.error(" [guzzler] Unknown message received: " + unknown)
      }
    }
    case unknown => logger.error(" [guzzler] Unknown message received: " + unknown)
  }
}
