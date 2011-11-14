package com.geishatokyo.helenos

import connection.{SimpleConnectionPool, OneTimeSessionPool, Session}
import org.apache.log4j.BasicConfigurator


/**
 * 
 * User: takeshita
 * Create: 11/11/09 13:17
 */

object SessionInitializer {

  BasicConfigurator.configure


  /**
   * dummy init method
   */
  def init() : Unit = { init("Keyspace1")}
  def init(defaultKeyspace : String) : Unit = {
    Session.init(defaultKeyspace,new OneTimeSessionPool(new SimpleConnectionPool("localhost",9160)))
  }
}