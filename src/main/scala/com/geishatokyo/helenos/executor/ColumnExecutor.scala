package com.geishatokyo.helenos.executor

import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.conversions.StandardPreDefs
import com.geishatokyo.helenos.command._
import com.geishatokyo.helenos.column.{ColumnName, Column}

/**
 * 
 * User: takeshita
 * Create: 11/11/11 13:21
 */

abstract class ColumnExecutor[T <: ColumnName](protected val column : T) {

  def :=( value : Array[Byte]) : Unit = {

    val command = new InsertColumn(column,value)
    Session.borrow(column.keyspace)(session => {
      command.execute(session)
    } )

  }

  def counter : Long = {
    _get match{
      case Some(c) => StandardPreDefs.long(c.value)
      case None => 0L
    }
  }

  def get : Array[Byte] = {
    _get match{
      case Some(c) => c.value
      case None => null
    }
  }
  def getOrElse( default : Array[Byte]) : Array[Byte] = {
    getOp getOrElse default
  }

  def getOp : Option[Array[Byte]] = {
    _get map( _.value)
  }
  protected def _get : Option[Column]


  def del : Boolean = {
    Session.borrow(column.keyspace)(session => {
      new RemoveColumn(column).execute(session)
    })
  }

  def +=( value : Int) : Unit = {
    Session.borrow(column.keyspace)(session => {
      new IncrementCounter(column,value).execute(session)
    })
  }

  def resetCounter : Unit = {
    Session.borrow(column.keyspace)(session => {
      new RemoveCounter(column)
    })
  }

  def -=(value : Int) : Unit = this.+=(-value)





}