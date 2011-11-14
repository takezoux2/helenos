package com.geishatokyo.helenos.command

/**
 * 
 * User: takeshita
 * Create: 11/11/09 23:13
 */


object AddResult extends Enumeration{
  val Add = Value(1,"Add")
  val Update = Value("Update")
  val Drop = Value("Drop")
  val DropAndAdd = Value("DropAndAdd")
  val Nothing = Value("Nothing")
}