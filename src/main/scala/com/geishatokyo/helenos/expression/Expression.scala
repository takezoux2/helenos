package com.geishatokyo.helenos.expression

import org.apache.cassandra.thrift.IndexOperator

/**
 * 
 * User: takeshita
 * Create: 11/09/19 13:12
 */

trait Expression{

  def columnName : Array[Byte]

  def operator : IndexOperator = IndexOperator.EQ

  def value : Array[Byte]

}