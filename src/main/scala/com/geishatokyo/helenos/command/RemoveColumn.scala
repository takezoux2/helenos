package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import org.apache.cassandra.thrift.ConsistencyLevel
import com.geishatokyo.helenos.TimeUtil
import com.geishatokyo.helenos.column.{ColumnName, ColumnNameForSuper, ColumnNameForStandard}

/**
 * 
 * User: takeshita
 * Create: 11/11/11 0:18
 */

class RemoveColumn( columnName : ColumnName) extends Command[Boolean]{


  def execute(session: Session, consistencyLevel: ConsistencyLevel) : Boolean = {
    execute(session,TimeUtil.currentMicroSec,consistencyLevel)
  }

  def execute(session: Session, timestamp : Long,  consistencyLevel: ConsistencyLevel) : Boolean = {
    session().remove(columnName.key.key,
      toColumnPath(columnName),
      timestamp,
      consistencyLevel)
    true

  }
}
