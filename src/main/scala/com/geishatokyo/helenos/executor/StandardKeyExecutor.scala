package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.column._
import com.geishatokyo.helenos.command.{GetSuperSlice, GetStandardSlice, BatchMutateSuper, BatchMutateStandard}
import com.geishatokyo.helenos.connection.{SessionPool, Session}


trait ColumnContainerExecutor{

  implicit def columnsToMap( list : List[Column]) : Map[Array[Byte],Array[Byte]] = {
    Map(list.map(col => col.name -> col.value) :_*)
  }

}

/**
 * 
 * User: takeshita
 * Create: 11/11/11 0:50
 */

class StandardKeyExecutor(standardKey : StandardKey)(implicit sessionPool: SessionPool) extends ColumnContainerExecutor{

  @inline
  private def session[T]( func : Session => T) : T = {
    sessionPool.borrow(standardKey.keyspace)(func)
  }

  def :=( mutations : List[Mutation]) = {
    session(session => {
      new BatchMutateStandard(standardKey,mutations).execute(session)
    })
  }

  def get : Map[Array[Byte],Array[Byte]] = {
    limit(limit=100)
  }

  def slice( columnNames : Array[Byte]*) : Map[Array[Byte],Array[Byte]] = {
    session(session => {
      new GetStandardSlice(standardKey,
        new Columns(columnNames.toList)).execute(session)
    })
  }


  def range(start : Array[Byte],finish : Array[Byte],reversed : Boolean = false) : Map[Array[Byte],Array[Byte]] = {
    session(session => {
      new GetStandardSlice(standardKey,
        new Range(start,finish,reversed)).execute(session)
    })
  }

  def limit( start : Array[Byte] = Array(), limit : Int,reversed : Boolean = false) : Map[Array[Byte],Array[Byte]] = {
    session(session => {
      new GetStandardSlice(standardKey,new OffsetLimit(start,limit,reversed)).execute(session)
    })
  }


}

class SuperColumnExecutor(superColumn : SuperColumn)(implicit sessionPool: SessionPool) extends ColumnContainerExecutor {


  @inline
  private def session[T]( func : Session => T) : T = {
    sessionPool.borrow(superColumn.keyspace)(func)
  }

  def :=( mutations : List[Mutation]) = {
    session(session => {
      new BatchMutateSuper(superColumn,mutations).execute(session)
    })
  }

  def get : Map[Array[Byte],Array[Byte]] = {
    limit(limit=100)
  }

  def slice( columnNames : Array[Byte] *) : Map[Array[Byte],Array[Byte]] = {
    session(session => {
      new GetSuperSlice(superColumn,
        new Columns(columnNames.toList)).execute(session)
    })
  }

  def range( start : Array[Byte],finish : Array[Byte],reversed : Boolean = false) : Map[Array[Byte],Array[Byte]] = {
    session(session => {
      new GetSuperSlice(superColumn,
        new Range(start,finish,reversed)).execute(session)
    })
  }

  def limit( start : Array[Byte] = Array(), limit : Int, reversed : Boolean = false): Map[Array[Byte],Array[Byte]] = {
    session(session => {
      new GetSuperSlice(superColumn,
        new OffsetLimit(start,limit,reversed)).execute(session)
    })
  }

}