package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/06 18:47
 */

class SuperColumn(val key : Key, val name : Array[Byte]) extends ColumnContainer {

  def columnFamily = key.columnFamily

  def \( name : Array[Byte]) = {
    new ColumnNameForSuper(this,name)
  }

}