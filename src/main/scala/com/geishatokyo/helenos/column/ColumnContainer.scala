package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/11 18:23
 */

trait ColumnContainer {

  def keyspace = columnFamily.keyspace

  def columnFamily : ColumnFamily

}