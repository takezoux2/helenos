package com.geishatokyo.helenos.connection

import org.apache.cassandra.thrift.{ConsistencyLevel, Cassandra}
import com.geishatokyo.helenos.column.Keyspace
import com.geishatokyo.helenos.command.SetKeyspace


/**
 * SessionPool object
 * this is a just proxy.
 * You can change behavior replace inner pool calling init(SessionPool)
 */
object Session extends SessionPool{
  private var innerPool : SessionPool = new OneTimeSessionPool()

  var defaultKeyspace : Keyspace = new Keyspace("Keyspace1")


  def init( defKeyspace : String,  pool : SessionPool) = {
    this.innerPool = pool
    defaultKeyspace = new Keyspace(defKeyspace)
  }

  def pool = innerPool.pool


  def borrow[T](keyspace : Option[Keyspace])(func: (Session) => T) : T = {
    borrow(keyspace.getOrElse(defaultKeyspace).name)(func)
  }

  def createSession(keyspace : String) = innerPool.createSession(keyspace)


  def createSession() = innerPool.createSession()

  def returnSession(session: Session) = { innerPool.returnSession(session)}
}


class Session(val client : Cassandra.Iface){

  implicit def __thisSession : Session = this

  var consistencyLevel = ConsistencyLevel.QUORUM

  def apply() : Cassandra.Iface = client


}
