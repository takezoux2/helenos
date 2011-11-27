package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.CassandraException
import org.apache.cassandra.thrift.InvalidRequestException

/**
 * 
 * User: takeshita
 * Create: 11/11/06 19:41
 */

class SetKeyspace(keyspace :String) extends SystemCommand[Unit] {

  def execute(implicit session : Session) = {
    session().set_keyspace(keyspace)
  }
}

