package com.geishatokyo.helenos.converter.template

import com.geishatokyo.helenos.converter.ConvertTemplate
import com.geishatokyo.helenos.column.{Column, ColumnForStandard}

/**
 * 
 * User: takeshita
 * Create: 11/11/14 17:08
 */

abstract class ReflectionTemplate[T](val clazz : Class[T]) extends ConvertTemplate[T] {


  def createInstance() : T = {
    clazz.newInstance().asInstanceOf[T]
  }

  def convert(CFKey: Array[Byte], superColumnName: Option[Array[Byte]], list: Map[Array[Byte], Array[Byte]]) = {
    createInstance()
  }

  def convert(obj: T, list: Map[Array[Byte], Array[Byte]]) = {
    obj
  }

  def deconvert(obj: T) = {
    (null,Nil)
  }
}
