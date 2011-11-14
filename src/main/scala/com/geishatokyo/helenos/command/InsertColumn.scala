package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import org.apache.cassandra.thrift.ConsistencyLevel
import com.geishatokyo.helenos.column.{ColumnName, ColumnNameForStandard}


/**
 * 
 * User: takeshita
 * Create: 11/09/19 17:05
 */

class InsertColumn( column : ColumnName , value : Array[Byte] ) extends Command[Unit]{



  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {

    session().insert(column.key.key,
      toColumnParent(column),
      toColumn(column,value),
      consistencyLevel
      )
  }

  override def toString = {
    "Insert column " + column
  }
}
