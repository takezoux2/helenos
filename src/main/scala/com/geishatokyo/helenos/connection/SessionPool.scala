package com.geishatokyo.helenos.connection

import com.geishatokyo.helenos.column.Keyspace
import org.slf4j.LoggerFactory

/**
 * 
 * User: takeshita
 * Create: 11/11/08 11:36
 */

class OneTimeSessionPool(val pool : ConnectionPool) extends SessionPool{

  def this() = this(new SimpleConnectionPool)

  var defaultKeyspace : Keyspace = new Keyspace("Keyspace1")


  def createSession(keyspace : String) = {
    val client = pool.getClient
    logger.debug("use keyspace:" + keyspace)
    client.set_keyspace(keyspace)
    new Session(client)
  }


  def createSession() = {
    val client = pool.getClient
    new Session(client)
  }

  def returnSession(session: Session) = {
    pool.returnClient(session.client)
  }
}

trait SessionPool {
  val logger = LoggerFactory.getLogger(classOf[SessionPool] )

  var defaultKeyspace : Keyspace

  def pool : ConnectionPool

  /**
   *
   */
  def borrow[T](keyspace : Option[Keyspace])(func: (Session) => T) : T = {
    borrow(keyspace.getOrElse(defaultKeyspace).name)(func)
  }
  /**
   * Get session which is set to passed keyspace
   *
   * @param keyspace Keyspace name to set
   * @param func process function
   */
  def borrow[T](keyspace : String)( func : Session => T) : T = {
    val session = createSession(keyspace)
    try{
      func(session)
    }finally{
      returnSession(session)
    }
  }

  /**
   * Get session which is not set keyspace.
   */
  def systemBorrow[T](func : Session => T) : T = {
    val session = createSession()
    try{
      func(session)
    }finally{
      returnSession(session)
    }
  }

  def createSession(keyspace : String) : Session
  def createSession() : Session

  def returnSession(session : Session) : Unit
}