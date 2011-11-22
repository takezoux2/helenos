package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.command.GetSuperColumnsSlice
import com.geishatokyo.helenos.connection.{SessionPool, Session}
import com.geishatokyo.helenos.column.{ColumnForSuper, OffsetLimit, SuperKey}

/**
 * 
 * User: takeshita
 * Create: 11/11/14 16:05
 */

class SuperKeyExecutor(superKey : SuperKey)(implicit sessionPool: SessionPool) {

  def get : Map[Array[Byte],Map[Array[Byte],Array[Byte]]] = {
    val r = sessionPool.borrow(superKey.keyspace)(session => {
      new GetSuperColumnsSlice(superKey,OffsetLimit(new Array[Byte](0),100,false)).execute(session)
    })

    r.map( p => p._1 -> Map(p._2.map( c => c.name -> c.value) :_* ))
  }

}