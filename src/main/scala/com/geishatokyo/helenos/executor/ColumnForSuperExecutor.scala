package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.column.ColumnNameForSuper
import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.command.GetSuperColumn

/**
 * 
 * User: takeshita
 * Create: 11/11/11 13:20
 */

class ColumnForSuperExecutor(_column : ColumnNameForSuper) extends ColumnExecutor[ColumnNameForSuper](_column) {


  protected def _get = {
    Session.borrow(_column.keyspace)(session => {
      new GetSuperColumn(_column).execute(session)
    })
  }

}