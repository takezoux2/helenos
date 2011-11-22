package com.geishatokyo.helenos.converter

import com.geishatokyo.helenos.connection.SessionPool
import com.geishatokyo.helenos.conversions.{TypeConversions, ExecutorConversions}
import TypeConversions._
import com.geishatokyo.helenos.column._

/**
 * 
 * User: takeshita
 * Create: 11/11/22 10:15
 */

class Converter(templateRegistry : TemplateRegistry,
                 templateBuilder : TemplateBuilder,
                 pool : SessionPool) {

  object executorConvs extends ExecutorConversions {
    def sessionPool = pool
  }
  import executorConvs._


  def convert[T]( columnContainer : StandardKey)(implicit manifest : Manifest[T]) : T = {
    convert(columnContainer.key, columnContainer get)
  }
  def convert[T]( columnContainer : SuperColumn)(implicit manifest : Manifest[T]) : T = {
    convert(columnContainer.key.key,columnContainer.name, columnContainer get)
  }

  def convert[T](CFKey : Array[Byte], columns : Map[Array[Byte],Array[Byte]])(implicit manifest : Manifest[T]) : T = {
    val template = getTemplate(manifest.erasure)

    template.convert(CFKey,None,columns)
  }
  def convert[T](CFKey : Array[Byte],superColumnName:Array[Byte],columns : Map[Array[Byte],Array[Byte]])(implicit manifest : Manifest[T]) : T = {
    val template = getTemplate(manifest.erasure)

    template.convert(CFKey,Some(superColumnName),columns)
  }

  protected def getTemplate[T](clazz : Class[_]) : ConvertTemplate[T] = {
    val template = templateRegistry.get(clazz)

    if(template == null){
    }

    template.asInstanceOf[ConvertTemplate[T]]
  }



}