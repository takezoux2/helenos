package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/06 18:36
 */

class ColumnFamily(
  val keyspace : Option[Keyspace],
  val name : String){

  def this(name : String) = this(None,name)
  def this(ks : Keyspace,name : String) = this(Some(ks),name)

  def \( key : Array[Byte]) : StandardKey = {
    new StandardKey(this,key)
  }

  def \\( key : Array[Byte]) : SuperKey = {
    new SuperKey(this,key)
  }

  override def toString = {
    keyspace.toString + "/" + name
  }
}