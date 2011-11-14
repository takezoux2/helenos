package com.geishatokyo.helenos.column

import java.nio.ByteBuffer

/**
 * 
 * User: takeshita
 * Create: 11/11/06 18:38
 */

trait Key{

  def key : Array[Byte]
  def keyspace : Option[Keyspace] = columnFamily.keyspace

  def columnFamily : ColumnFamily

  override def toString = {
    columnFamily + "/" +  { try{
      new String(key , "utf-8")
    }catch{
      case e => {
        key.map(b => "%02x".format(b)).mkString("(",",",")")
      }
    } }
  }
}


class StandardKey(val columnFamily : ColumnFamily,
                  val key : Array[Byte]) extends Key with ColumnContainer{


  override def keyspace = columnFamily.keyspace

  def \( name : Array[Byte]) = {
    new ColumnNameForStandard(this,name)
  }



}

class SuperKey(val columnFamily : ColumnFamily,
                  val key : Array[Byte]) extends Key{

  def \( superColumnName : Array[Byte]) = {
    new SuperColumn(this,superColumnName)
  }

}