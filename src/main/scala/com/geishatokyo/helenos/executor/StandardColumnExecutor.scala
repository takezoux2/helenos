package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.column.{Column, ColumnNameForStandard}
import com.geishatokyo.helenos.conversions.StandardPreDefs
import com.geishatokyo.helenos.command._
import com.geishatokyo.helenos.connection.{SessionPool, Session, ConnectionPool}

/**
 * 
 * User: takeshita
 * Create: 11/11/06 20:11
 */

class StandardColumnExecutor(_column : ColumnNameForStandard)(implicit sessionPool: SessionPool) extends ColumnExecutor[ColumnNameForStandard](_column){

  protected def _get = {
    val command = new GetColumn(column)
    sessionPool.borrow(column.keyspace)(session => {
      command.execute(session)
    })
  }

}