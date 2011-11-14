package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import org.apache.cassandra.thrift.ConsistencyLevel
import com.geishatokyo.helenos.column.{ColumnName, ColumnNameForSuper, ColumnNameForStandard}

/**
 * 
 * User: takeshita
 * Create: 11/11/11 0:36
 */

class IncrementCounter(columnName : ColumnName,value : Long) extends Command[Boolean] {


  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    session().add(columnName.key.key,
      toColumnParent(columnName),
      toCounterColumn(columnName,value),
      consistencyLevel)
    true
  }
}


class RemoveCounter(columnName : ColumnName) extends Command[Boolean] {
  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    session().remove_counter(columnName.key.key,
      toColumnPath(columnName),
      consistencyLevel)
    true
  }
}
