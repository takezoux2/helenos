package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.connection.{Session, ConnectionPool}
import com.geishatokyo.helenos.column.{Column, ColumnNameForStandard}
import com.geishatokyo.helenos.conversions.StandardPreDefs
import com.geishatokyo.helenos.command._

/**
 * 
 * User: takeshita
 * Create: 11/11/06 20:11
 */

class StandardColumnExecutor(_column : ColumnNameForStandard) extends ColumnExecutor[ColumnNameForStandard](_column){

  protected def _get = {
    val command = new GetStandardColumn(column)
    Session.borrow(column.keyspace)(session => {
      command.execute(session)
    })
  }

}