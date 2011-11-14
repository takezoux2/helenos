package com.geishatokyo.helenos

/**
 * Util for String
 * User: takeshita
 * Create: 11/11/09 1:43
 */

object StringUtil {

  /**
   * Convert to String.
   * If failed, it returns hex array values.
   */
  def toString( maybeString : Array[Byte]) = {
    try{
      new String(maybeString)
    }catch{
      case e : Exception => {
        maybeString.map(b => "%02x".format(b)).mkString("[" , ",","]")
      }
    }
    //maybeString.map(b => "%02x".format(b)).mkString("[" , ",","]")

  }


  /**
   * Make an initial character to upper case.
   */
  def toInitialUpperCase(v : String) = {
    v.charAt(0).toUpper + v.substring(1)
  }
}