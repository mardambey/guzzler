import sbt._
import Keys._

object BuildSettings {
  val buildOrganization = "mardambey.net"
  val buildVersion      = "1.0"
  val buildScalaVersion = "2.9.0-1"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    shellPrompt  := ShellPrompt.buildShellPrompt
  )
}

// Shell prompt which show the current project, 
// git branch and build version
object ShellPrompt {
  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }
  
  val current = """\*\s+([\w-]+)""".r
  
  def gitBranches = ("git branch --no-color" lines_! devnull mkString)
  
  val buildShellPrompt = { 
    (state: State) => {
      val currBranch = 
        current findFirstMatchIn gitBranches map (_ group(1)) getOrElse "-"
      val currProject = Project.extract (state).currentProject.id
      "%s:%s:%s> ".format (
        currProject, currBranch, BuildSettings.buildVersion
      )
    }
  }
}

object Resolvers {
	val GuzzlerResolvers = Seq(
		"Local Maven Repository" at "file://" + Path.userHome + "/.m2/repository",
		JavaNet1Repository
	)
}

object Dependencies {
	val configgy = "net.lag" % "configgy" % "2.0.0" intransitive()
  val slf4j = "org.slf4j" % "slf4j-simple" % "1.6.1"
  val rabbitmqClient = "com.rabbitmq" % "amqp-client" % "2.5.1"
}

object GuzzlerBuild extends Build {
  import Resolvers._
  import Dependencies._
  import BuildSettings._

	val deps = Seq (
		configgy, slf4j, rabbitmqClient, asyncHttp, liftJson
	)

  lazy val guzzler = Project (
    "Guzzler",
    file ("."),
    settings = buildSettings ++ Seq (resolvers := GuzzlerResolvers, libraryDependencies ++= deps)
  )
}
