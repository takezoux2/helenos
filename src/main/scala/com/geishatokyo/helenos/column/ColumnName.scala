package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/06 19:47
 */

trait ColumnName {

  def name : Array[Byte]


  def keyspace : Option[Keyspace] = columnFamily.keyspace

  def key : Key
  def columnFamily : ColumnFamily = key.columnFamily


}

class ColumnNameForStandard(_key : StandardKey,
                            val name : Array[Byte]) extends ColumnName{

  def key = _key

  override def toString = {
    key + "/" + { try{
      new String(name , "utf-8")
    }catch{
      case e => {
        name.map(b => "%02x".format(b)).mkString("(",",",")")
      }
    } }
  }

}

class ColumnNameForSuper(val superColumn : SuperColumn,
                          val name : Array[Byte]) extends ColumnName{


  def key = superColumn.key

}

