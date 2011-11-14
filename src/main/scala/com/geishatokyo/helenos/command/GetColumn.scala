package com.geishatokyo.helenos.command

import java.util.Date
import com.geishatokyo.helenos.connection.Session
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{NotFoundException, ColumnPath, ConsistencyLevel}
import com.geishatokyo.helenos.column._


object GetColumn{
  def apply( column : ColumnNameForStandard) : GetStandardColumn = {
    new GetStandardColumn(column)
  }
}
/**
 * 
 * User: takeshita
 * Create: 11/09/19 15:44
 */

class GetStandardColumn(val columnName : ColumnNameForStandard) extends Command[Option[ColumnForStandard]]{
  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    try{
      val r = session().get(columnName.key.key,
      columnName,
      consistencyLevel)
      Option(r)
    }catch{
      case e : NotFoundException => {
        logger.debug("Column:%s is not found.".format(columnName.toString))
        None
      }
    }

  }
}


class GetSuperColumn(val columnName : ColumnNameForSuper) extends Command[Option[ColumnForSuper]]{
  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    try{
      val r = session().get(columnName.key.key,
      columnName,
      consistencyLevel)
      Option(r)
    }catch{
      case e : NotFoundException => {
        logger.debug("Column:%s is not found.".format(columnName.toString))
        None
      }
    }

  }
}
