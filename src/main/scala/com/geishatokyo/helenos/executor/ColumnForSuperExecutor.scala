package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.column.ColumnNameForSuper
import com.geishatokyo.helenos.connection.{SessionPool, Session}
import com.geishatokyo.helenos.command.{GetColumn}

/**
 * 
 * User: takeshita
 * Create: 11/11/11 13:20
 */

class ColumnForSuperExecutor(_column : ColumnNameForSuper)(implicit sessionPool: SessionPool) extends ColumnExecutor[ColumnNameForSuper](_column) {


  protected def _get = {
    sessionPool.borrow(_column.keyspace)(session => {
      new GetColumn(_column).execute(session)
    })
  }

}