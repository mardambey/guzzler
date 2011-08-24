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
import actors.Actor
import net.lag.configgy.Configgy

object Config {

  var config:net.lag.configgy.Config = _
  val log = Logger.get
  var defaultContentType:String = ""
  val consumers = scala.collection.mutable.Set[Actor]()

  var mysqlHost:Option[String] = None
  var mysqlUser:Option[String] = None
  var mysqlPassword:Option[String] = None
  var mysqlDb:Option[String] = None
  var mysqlCmd:Option[String] = None
  var mysqlBinlogStreamer:Option[String] = None
  var mysqlSlaveServerId:Option[String] = None

  val DEFAULT_CONSUMER_PACKAGE = ""

  def load(file:String) : Boolean = {
    Configgy.configure(file)
    config = Configgy.config

    mysqlHost = config.getString("mysqlHost")
    mysqlUser = config.getString("mysqlUser")
    mysqlPassword = config.getString("mysqlPassword")
    mysqlDb = config.getString("mysqlDb")
    mysqlCmd = config.getString("mysqlCmd")
    mysqlBinlogStreamer = config.getString("mysqlBinlogStreamer")
    mysqlSlaveServerId = config.getString("mysqlSlaveServerId")

    // get list holding consumers
    val consumerCfg = config.getList("consumers")

    // load the default consumer package (optional, blank used of not provided)
    val defaultConsumerPackage = config.getString("default-consumer-package", DEFAULT_CONSUMER_PACKAGE)

    consumerCfg.foreach (c => {
      log.ifDebug("Loading consumer: " + defaultConsumerPackage + "." + c)
      try {
      val actor = Class.forName(defaultConsumerPackage + "." + c).newInstance().asInstanceOf[Actor]
      actor.start()
      consumers += actor
      } catch {
        case e:Exception => log.ifError(e, "Error loading consumer " + c)
      }
    })

    true
  }
}