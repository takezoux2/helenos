package com.geishatokyo.helenos.command

import org.apache.cassandra.thrift.{Cassandra, ConsistencyLevel}
import com.geishatokyo.helenos.connection.{StructureConversions, Session, Settings}
import org.slf4j.LoggerFactory

/**
 * Build end execute command
 * User: takeshita
 * Create: 11/09/19 15:13
 */

trait Command[T] extends StructureConversions {

  def logger = CommandLogger

  var cons : ConsistencyLevel = Settings.defaultReadConsistency
  def consistency( cons : ConsistencyLevel) : this.type = {
    this.cons = cons
    this
  }

  def execute(implicit session : Session) : T = execute(session,session.consistencyLevel)
  def execute( session : Session,consistencyLevel : ConsistencyLevel) : T



}

trait SystemCommand[T]{
  def logger = CommandLogger
  def execute(implicit session : Session) : T
}