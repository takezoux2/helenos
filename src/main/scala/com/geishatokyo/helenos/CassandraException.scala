package com.geishatokyo.helenos

/**
 * 
 * User: takeshita
 * Create: 11/11/06 20:06
 */

class CassandraException(_message : String, _e : Throwable) extends Exception(_message,_e) {

  def this( _m : String) = this(_m,null)

}