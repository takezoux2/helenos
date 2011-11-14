package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.command.GetSuperColumnsSlice
import com.geishatokyo.helenos.column.{OffsetLimit, SuperKey}

/**
 * 
 * User: takeshita
 * Create: 11/11/14 16:05
 */

class SuperKeyExecutor(superKey : SuperKey) {

  def get : Map[Array[Byte],Map[Array[Byte],Array[Byte]]] = {
    Session.borrow(superKey.keyspace)(session => {
      new GetSuperColumnsSlice(superKey,OffsetLimit(new Array[Byte](0),100,false)).execute(session)
    })

  }

}