package guzzler

import scala.sys.process.Process
import scala.sys.process.ProcessIO
import net.lag.logging.Logger
import akka.actor.Actor
import akka.dispatch.Future
import akka.util.duration._
import akka.agent.Agent
import java.util.concurrent.BlockingQueue
import java.util.LinkedList
import java.io.{InputStream, InputStreamReader, BufferedReader}
import java.lang.{Boolean, ProcessBuilder, Process => JProcess}

// shows the state of the queue
 case class QueueState()

// restarts the binlog streaming process
case class StreamRestart()

// start binlog streaming from the given file and position
case class StreamSeek(binlogFile:String, binlogPosition:Long)

// start binlog streaming
case class StreamStart()

// stops binlog streaming
case class StreamStop()

// stream another line
case class StreamNext()

// resumes streaming from the last known position
case class StreamResume()

// resets everything to starting defaults
case class StreamReset()

//
// TODO: redefine all "atomic" operations
// start, stop. seek, etc

/**
 * Runs mysqlbinlog and reads the input line by line. If position
 * markers are encountered they are used for check-pointing. Lines
 * are pushed into a blocking queue.
 */
class GuzzlerBinlogStreamer(queue:BlockingQueue[String]) extends Actor {

  // logger object
  val logger = Logger.get

  // if true, indicates that Guzzler should iterate
  // over the binlog stream and read lines
  var fetchLines = Agent(false)

  // if set to true then Guzzler will force
  // the streaming to start (or stop) and kill the old
  // stream  if it is there
  var forceStartStop = false

  // default values for the seekFile
  // and the seekPosition
  val SEEK_FILE_EMPTY = ""
  val SEEK_POSITION_EMPTY = -1L

  // if specified, binlog streaming will start
  // from this file
  var seekFile = SEEK_FILE_EMPTY

  // if specified, binlog streaming will start
  // from this position
  var seekPosition = SEEK_POSITION_EMPTY

  val sqlRegex = """(?i)(insert |update |delete ).*""".r

  val atRegex = """(?i)# at (\d+)""".r

  var process:JProcess = _

  var stdout:InputStream = _

  var bufferedReader:Option[BufferedReader] = None

  override def preStart() {

    val (file, position) = getBinlogFileAndPosition

    logger.info(" [guzzler] Binlog streamer initializing.")
    self ! StreamSeek(file, position.toLong)
    self ! StreamStart()
  }

  /**
   * If seekFile and seekPosition are set, use them,
   * otherwise, find their current values from the db
   * specified in the Config.
   */
  def getBinlogFileAndPosition : (String, String) = {
    if (!seekFile.equals(SEEK_FILE_EMPTY) && seekPosition > SEEK_POSITION_EMPTY) {
      (seekFile, seekPosition.toString)
    } else {
      getBinLogCurrentPosition.get
    }
  }

  /**
   * Run mysqlbinlog starting from the given
   * file name and file position, skipping as
   * many records as offset specified, defaults
   * to zero.
   * TODO: handle case where we can't seek
   */
  def binlogSeek(file:String, position:Long, offset:Int = 0) {

    // build and run mysql binlog streamer, possibly resuming from a certain position
    logger.info(" [guzzler] Seeking to binlog " + file + " at position " + position + ", skipping " + offset + " records.")

    val binLogStreamCmd = Config.mysqlBinlogStreamer.get
    val args = List(
      binLogStreamCmd,
      "-h" + Config.mysqlHost.get,
      "-P" + Config.mysqlPort.get,
      "-u" + Config.mysqlUser.get,
      "-p" + Config.mysqlPassword.get,
      "-R", "--start-position=" + position,
      "--stop-never",
      "--stop-never-slave-server-id=" + Config.mysqlSlaveServerId.get ,
      "-f", file,
      "--offset=" + offset)

    try {

      seekFile = file
      seekPosition = position

      // use the args above to build a process
      val processBuilder = new ProcessBuilder(
        args.foldLeft(new LinkedList[String]())((list, w) => { list.add(w); list }))
      process = processBuilder.start()
      stdout = process.getInputStream
      bufferedReader = Some(new BufferedReader(new InputStreamReader(stdout)))
    } catch {
      case e:Exception => {
        logger.error(e, " [guzzler] Could not seek to binlog " + file + " at position " + position)
        bufferedReader = None
      }
    }
  }

  /**
   * Reads lines from the binary log. We should
   * already have called seek to the proper location.
   */
  def binlogReadLines() {
    bufferedReader match {
      case Some(br) => {
        val line = binlogReadLine(bufferedReader.get)
        val notTimeoutedOut = binlogConsumeLine(line)

        // if we havent timed out try to
        // read the next line
        if (notTimeoutedOut) {
          if (fetchLines())
            self ! StreamNext()
          else {
            // someone stopped us while reading
            logger.info(" [guzzler] Stopping binlog streaming process at " + seekFile + " position " + seekPosition)
            bufferedReader.get.close()
            bufferedReader = None
            process.destroy()
          }
        } else {
          // being here means we've timed out while
          //trying to read from mysqlbinlog

          fetchLines.send(false)
          process.destroy()
          logger.info(" [guzzler] Caught timeout while reading from mysqlbinlog at position " + seekPosition)

          // we're at snapshotPosition, try to resume from there, if we cant,
          // get the current file + pos and resume from there
          Thread.sleep(2000) // grace period
          self ! StreamSeek(seekFile, seekPosition)
          self ! StreamStart()
        }
      }
      case None => {
        logger.error(" [guzzler] Could not read lines from binlog " + seekFile + " at position " + seekPosition)
      }
    }
  }

  def binlogConsumeLine(line:Option[String]) : Boolean = {
    line match {
      case None => {
        // timeout
        false
      }
      case Some(str) => {
        try {
          str match {
            case sqlRegex(sql) => {
              queue.put(str)
            }
            case atRegex(position) => {
              seekPosition = position.toLong
            }
            case ignore =>
          }
        } catch {
          case _ =>
        }
        true
      }
      case _ => true
    }
  }

  /**
   * Attempts to read a line from the binlog stream.
   * Times out the read based on the Config, this
   * causes a reconnect attempt.
   *
   * Returns empty string if the buffered reader get a
   * null, returns the line if it could be read, and
   * None if the read times out.
   *
   * TODO: make this retry first before reconnecting
   */
  def binlogReadLine(bufferedReader:BufferedReader) : Option[String] = {
    val f = Future {
      bufferedReader.readLine() match {
        case null => Some("")
        case str => Some(str)
      }
    }

    try {
      f.await(2.seconds)
      f.get
    } catch {
      case e:Exception => logger.error(e, " [guzzler] Timeout while reading line from binlog.")
      None
    }
  }

  /**
   * Stop reading from mysqlbinlog and
   * terminates the process.
   */
  def binlogStop() {

    logger.info(" [guzzler] About to stop streaming binlogs.")

    // stop fetching lines
    fetchLines.send(false)
  }

  def binlogReset() {

    logger.info(" [guzzler] Resetting stream state.")

    fetchLines.send(false)

    bufferedReader match {
      case Some(bf) => bf.close()
      case _ =>
    }

    process match {
      case null =>
      case p => process.destroy()
    }

    bufferedReader = None
    process = null

    seekFile = SEEK_FILE_EMPTY
    seekPosition = SEEK_POSITION_EMPTY
  }

  def die(msg:String, e:Option[Exception] = None) {
    e match {
      case Some(ex) => logger.error(ex, msg)
      case any => logger.error(msg)
    }

    sys.exit(-1)
  }

  /**
   * Queries the MySQL server for the current file and
   * position of its binlog.
   */
  def getBinLogCurrentPosition : Option[(String, String)] = {
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
        binLogProc.exitValue()
        val binLogFilePosition = binLogStdout.toString.split("Binlog_Ignore_DB")(1).split("""\s+""").slice(0, 2)
        Some((binLogFilePosition(0), binLogFilePosition(1)))
      }
      case error => {
        None
      }
    }
  }

  def receive = {
    // Start streaming. In order for this to work we
    // must have seeked() before.
    case StreamStart() => {
      bufferedReader match {
        case None => logger.error(" [guzzler] Can not start streaming binlog is not seeked (current seek valies: " + seekFile + " at " + seekPosition + ")")
        case _ => {
          fetchLines() match {
            case true => logger.error(" [guzzler] Can not start streaming if another stream is already active (" + seekFile + " at " + seekPosition + ")")
            case _ => {
              fetchLines.send(true)
              binlogReadLines()
            }
          }
        }
      }
    }

    // Stops streaming. This finishes reading the current
    // line and terminates the streaming process.
    case StreamStop() => {
      bufferedReader match {
        case None => logger.error(" [guzzler] Can not stop streaming if no stream is active.")
        case _ => binlogStop()
      }
    }

    // Seeks to the specified binlog file and postition.
    // Does not start streaming, can not be called while
    // a streaming process is running.
    case StreamSeek(file, position) => {
      bufferedReader match {
        case None => binlogSeek(file, position)
        case _ => logger.error(" [guzzler] Can not seek if stream is already active.")
      }
    }

    // Reads the next line from the already
    // open stream and seeked to stream.
    case StreamNext() => {
      bufferedReader match {
        case None => logger.error(" [guzzler] Can not fetch next line if stream is not active.")
        case _ => binlogReadLines()
      }
    }

    // Resumes the streaming from the last
    // known good position.
    case StreamResume() => {
      self ! StreamSeek(seekFile, seekPosition)
      self ! StreamStart()
    }

    // Resets the stream to its default
    // state in case of a lock-up.
    case StreamReset() => {
      binlogReset()
    }


    case unknown => logger.error(" [guzzler] Unknown message received: " + unknown)
  }
}
