package com.geishatokyo.helenos.converter.template

import com.geishatokyo.helenos.converter.ConvertTemplate
import com.geishatokyo.helenos.column.{Column, ColumnForStandard}

/**
 * 
 * User: takeshita
 * Create: 11/11/14 17:08
 */

class ReflectionTemplate[T](val clazz : Class[T]) extends ConvertTemplate[T] {

  def createInstance() : T = {
    clazz.newInstance().asInstanceOf[T]
  }

  def convert(list: List[Column]) = {
    createInstance()
  }

  def deconvert(obj: T) = {
    (null,Nil)
  }
}
