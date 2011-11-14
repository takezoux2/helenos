package com.geishatokyo.helenos.converter

import com.geishatokyo.helenos.column._


/**
 * Convert standard columns to object
 * User: takeshita
 * Create: 11/11/14 17:03
 */

trait ConvertTemplate[T] {

  def convert(list : List[Column]) : T
  def deconvert( obj : T) : (ColumnContainer,List[Mutation])

}