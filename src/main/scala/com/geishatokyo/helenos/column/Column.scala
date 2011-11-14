package com.geishatokyo.helenos.column

import com.geishatokyo.helenos.StringUtil

/**
 * 
 * User: takeshita
 * Create: 11/11/06 18:38
 */

trait Column {

  def name : Array[Byte]
  def value : Array[Byte]
  def timestamp : Long

  override def toString = {
    StringUtil.toString(name) + ":" + StringUtil.toString(value)
  }
}


class ColumnForStandard(
  val name : Array[Byte],
  val value : Array[Byte],
  val timestamp : Long) extends Column{



}

class ColumnForSuper(
  val superColumnName : Array[Byte],
  val name : Array[Byte],
  val value : Array[Byte],
  val timestamp : Long) extends Column{
}

