package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/06 18:36
 */

class Keyspace(val name : String) {

  def @@( columnFamily : String) : ColumnFamily = {
    new ColumnFamily(this,columnFamily)
  }

  override def equals(obj: Any) = {
    obj match{
      case ks : Keyspace => ks.name == name
      case _ => false
    }
  }

  override def toString = "Keyspace[" + name + "]"
}