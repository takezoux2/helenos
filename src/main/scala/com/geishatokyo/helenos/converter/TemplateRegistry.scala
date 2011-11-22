package com.geishatokyo.helenos.converter

import java.util.concurrent.ConcurrentHashMap

/**
 * 
 * User: takeshita
 * Create: 11/11/22 10:14
 */

class TemplateRegistry {

  val templates = new java.util.concurrent.ConcurrentHashMap[Class[_],ConvertTemplate[_]]()

  def register( clazz : Class[_] , template : ConvertTemplate[_]) = {
    templates.put(clazz,template)
  }

  def get(clazz : Class[_]) = templates.get(clazz)

  def unregister(clazz : Class[_]) = {
    templates.remove(clazz)
  }

  def clear() = templates.clear()


}