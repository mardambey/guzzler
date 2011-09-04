package guzzler

import scala.sys.process.Process
import scala.sys.process.ProcessIO
import net.lag.logging.Logger
import akka.actor.Actor
import akka.dispatch.Future
import akka.util.duration._
import java.io.{InputStreamReader, BufferedReader}
import java.util.concurrent.BlockingQueue

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

/**
 * Runs mysqlbinlog and reads the input line by line. If position
 * markers are encountered they are used for check-pointing. Lines
 * are pushed into a blocking queue.
 */
class GuzzlerBinlogStreamer(queue:BlockingQueue[String]) extends Actor {

  // logger object
  val logger = Logger.get

  // holds the current binlog position
  var snapshotPosition:Long = -1

  // if true, Guzzler will keep trying to stream
  // binlogs unless it fails a max amount of times
  // (in Config) in a row.
  var stream = true

  // if true, indicates that Guzzler should iterate
  // over the binlog stream and read lines
  var fetchLines = true

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

  // future holding streamer
  var streamer:Option[Future[Unit]] = None

  override def preStart() {
    logger.info(" [guzzler] Binlog streamer initializing.")
    startStreaming(seekFile, seekPosition)
  }
  /**
   * Streams the binlogs (as per Config) from the given file and position. If
   * skipRecord is specified, the first record will be skipped.
   */
  def streamBinLog(binLogFilePosition:Array[String], skipRecord:Boolean = false) : Boolean = {
    try {
      // build and run mysql binlog streamer, possibly resuming from a certain position
      logger.info(" [guzzler] Starting from binlog " + binLogFilePosition(0) + " at position " + binLogFilePosition(1))

      val binLogStreamCmd = Config.mysqlBinlogStreamer.get
      val args = List(
        binLogStreamCmd,
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

      //val binLogStreamProcBuilder = Process(binLogStreamCmd, args)

      val sqlRegex = """(?i)(insert |update |delete ).*""".r
      val atRegex = """(?i)# at (\d+)""".r

      val processBuilder = new java.lang.ProcessBuilder(
        args.foldLeft(new java.util.LinkedList[String]())((list, w) => { list.add(w); list }))

      val process = processBuilder.start()
      val stdout = process.getInputStream

      val bufferedReader = new BufferedReader(new InputStreamReader(stdout))
      var line = readLine(bufferedReader)

      logger.info(" [guzzler] Guzzling down binary logs *guzzle* *guzzle*")

      fetchLines = true

      while (fetchLines) {
        line match {
          case None => {
            // timeout
            fetchLines = false
            process.destroy()
            throw new Exception(" [guzzler] Caught timeout while reading from mysqlbinlog at position " + snapshotPosition)
          }
          case Some(str) => {
            try {
              str match {
                case sqlRegex(sql) => {
                  queue.put(str)
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

      logger.info(" [guzzler] Stopped fetching lines from binlog stream.")
      true
    } catch {
      case e:Exception => {
        logger.error(e, " [guzzler] Could not stream binary logs.")
        true
      }
    }
  }

  def die(msg:String, e:Option[Exception] = None) {
    e match {
      case Some(ex) => logger.error(ex, msg)
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
        binLogProc.exitValue()
        val binLogFilePosition = binLogStdout.toString.split("Binlog_Ignore_DB")(1).split("""\s+""").slice(0, 2)
        Some(binLogFilePosition)
      }
      case error => {
        None
      }
    }
  }

  /**
   * Attempts to read a line from the binlog stream.
   * Times out the read based on the Config, this
   * causes a reconnect attempt.
   * TODO: make this retry first before reconnecting
   */
  def readLine(bufferedReader:BufferedReader) : Option[String] = {
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

  def stream(binLogFile:String, binLogPosition:Long) {

    // if given a binlog file and position, use them,
    // otherwise, find them from the db specified in the Config
    val binLogFilePosition = if (!binLogFile.equals(SEEK_FILE_EMPTY) && binLogPosition > SEEK_POSITION_EMPTY) {
      Array[String](binLogFile, binLogPosition.toString)
    } else {
      // find the current name and position of the binlog to stream
      getBinLogCurrentPosition.getOrElse({die("Could not get current binlog position.")}).asInstanceOf[Array[String]]
    }

    // if set to true, the first record will be
    // skipped using --offset=1 in mysqlbinlog
    // this is used when theres a recovery from
    // a crash or a restart of the streamer
    var skipRecord = false

    stream = true

    // stream binlogs until interrupted
    // TODO: can this be turned into just an actor
    // sending and receiving messages with a future
    while (stream) {
      // if we're here then we either need to
      // restart or we need to exit
      streamBinLog(binLogFilePosition, skipRecord) match {
        case true if stream => {

          var keepTrying = true
          var results:Option[Array[String]] = None

          for (i <- 1 to Config.maxReconnectAttempts.get if keepTrying) {
            logger.info(" [guzzler] Determining location to stream from (attempt " + i + " or " + Config.maxReconnectAttempts.get + ")")

            // if we're asked to start from a specific location, ie seeking,
            // then use that provided position
            if (seekFile.equals(SEEK_FILE_EMPTY) && seekPosition > SEEK_POSITION_EMPTY) {
               results = Some(Array[String](seekFile, seekPosition.toString))
            } else {
              val f = Future[Array[String]] { getBinLogCurrentPosition.get }
              results = try {
                // TODO: make timeout configurable
                f.await(2.seconds)
                keepTrying = false
                Some(f.get)
              } catch {
                case e:Exception => {
                  logger.error(e, " [guzzler] Could not determine binlog position.")
                  None
                }
              }
            }
          }

          results match {
            case None => die("Could not determine the current binary log position, last good position is " + binLogFilePosition(0) + " -> " + binLogFilePosition(1))
            case _ =>
          }

          // get current position of binlog
          val curBinLogPosition = results.get

          // use the current filename
          binLogFilePosition(0) = curBinLogPosition(0)

          // use the last saved position unless a seekPosition
          // was asked for
          binLogFilePosition(1) = if (seekPosition > SEEK_POSITION_EMPTY) seekPosition.toString else snapshotPosition.toString

          // reset seek file and position
          // in case they were spcified
          seekFile = SEEK_FILE_EMPTY
          seekPosition = SEEK_POSITION_EMPTY

          logger.info(" [guzzler] Resuming from binlog file " + binLogFilePosition(0) +
            " from position " + binLogFilePosition(1) +
            " (current server position: " + curBinLogPosition(1) + ")")

          // grace period
          // TODO: make configurable
          Thread.sleep(2000)
        }
        case false => {
          stream = false
          die(" [guzzler] Not resuming, exiting.")
        }
        // we're being asked to stop streaming
        // since "stream" is false
        case true => {
          logger.info(" [guzzler] Leaving main streaming loop.")
        }
      }

      // being here means we either
      // crashed or were restarted,
      // in both cases skip the first
      // record that is received
      skipRecord = true
    }
  }

  def startStreaming(binLogFile:String, binLogPosition:Long) {
    streamer = Some(Future {
      stream(binLogFile, binLogPosition)
    })
  }

  /**
   * Stops streaming if we're currently streaming..
   * If we not streaming and we're not being forced to stop
   * then ignore this request, otherwise acts as a form of a
   * kill to the streaming request.
   */
  def stopStreaming() {
    if (!stream && !forceStartStop) logger.info(" [guzzler] Asked to stop streaming while already stopped, refusing, use \"force\" to override.")
    else {
      logger.info(" [guzzler] About to stop streaming binlogs.")

      // stop main streaming loop from reconnecting
      stream = false

      // stop fetching lines
      fetchLines = false

      // wait for the streamer to exit or kill it
      // if it times out
      // TODO: make this configurable
      try { streamer.get.await(2.seconds) } catch { case e:Exception => logger.error(e, " [guzzler] Streamer did not stop, timed out after 2 seconds.")}

      streamer = None
    }
  }

  /**
   * Starts streaming if we're not already doing so.
   * If we are and we're not being forced to do so
   * then ignore this request.
   */
  def maybeStartStreaming() {
    if (stream && !forceStartStop) logger.info(" [guzzler] Asked to start streaming while already active, refusing, use \"force\" to override.")
    else {

      // if streaming, stop
      maybeStopStreaming()

      val curBinLoFileAndPos = getBinLogCurrentPosition.get
      // reset force start in case it was used
      forceStartStop = false

      if (snapshotPosition > -1) {
        // TODO: do not "get" this blindly
        startStreaming(curBinLoFileAndPos(0), snapshotPosition)
      } else {
        startStreaming(curBinLoFileAndPos(0), curBinLoFileAndPos(1).toLong)
      }
    }
  }

  /**
   * If we're streaming, stop doing so.
   */
  def maybeStopStreaming() {
    if (streamer != None) stopStreaming()
  }

  /**
   * Attempts to seek to the given file and
   * position in the binlog stream.
   */
  def seekStream(file:String, position:Long) {
    logger.info(" [guzzler] Received request to seek to binlog file " + file + " and position " + position)
    maybeStopStreaming()
    startStreaming(file, position)
  }

  def receive = {
    case StreamStart() => maybeStartStreaming()
    case StreamStop() => stopStreaming()
    case StreamSeek(file, position) => seekStream(file, position)
    case StreamRestart() => fetchLines = false
    //case QueuePause() => queue ! QueuePause()
    //case QueueResume() => queue.restart()
    case unknown => logger.error(" [guzzler] Unknown message received: " + unknown)
  }
}
