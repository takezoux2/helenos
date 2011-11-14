package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/11 18:33
 */

trait Predicate {

}

case class Columns( columns : List[Array[Byte]]) extends Predicate

case class Range(start : Array[Byte] , finish : Array[Byte] , reversed : Boolean ) extends Predicate{
  def this(start : Array[Byte] , finish : Array[Byte]) = this(start,finish,false)
}

case class OffsetLimit(start : Array[Byte], count : Int , reversed : Boolean) extends Predicate{
  def this(start : Array[Byte] , count : Int) = this(start,count,false)
  def this(count : Int) = this(Array(),count,false)
}