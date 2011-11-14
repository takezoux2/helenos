package com.geishatokyo.helenos.converter.template

import com.geishatokyo.helenos.conversions.Serializer
import java.lang.reflect.Method

/**
 * 
 * User: takeshita
 * Create: 11/11/14 17:25
 */

trait FieldTemplate[T] {

  def columnName : Array[Byte]
  def method : Method
  def serializer : Serializer[T]
}
