package com.geishatokyo.helenos.column

/**
 * 
 * User: takeshita
 * Create: 11/11/11 0:46
 */

trait Mutation

case class Insertion(name : Array[Byte], value : Array[Byte]) extends Mutation

case class Deletion(name : Array[Byte]) extends Mutation

case class Increment( name : Array[Byte] , value : Long) extends Mutation

