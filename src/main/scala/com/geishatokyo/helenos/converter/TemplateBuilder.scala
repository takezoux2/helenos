package com.geishatokyo.helenos.converter

/**
 * 
 * User: takeshita
 * Create: 11/11/22 10:15
 */

trait TemplateBuilder {

  def buildTemplate(clazz : Class[_]) : ConvertTemplate[_]

}