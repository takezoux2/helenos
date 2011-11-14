package com.geishatokyo.helenos.conversions

import com.geishatokyo.helenos.column._
import com.geishatokyo.helenos.executor._

/**
 * 
 * User: takeshita
 * Create: 11/11/11 1:18
 */

trait ExecutorConversions {


  //for command

  implicit def columnToExecutor( column : ColumnNameForStandard) = {
    new StandardColumnExecutor(column)
  }
  implicit def columnForSuperToExecutor( column : ColumnNameForSuper) = {
    new ColumnForSuperExecutor(column)
  }

  implicit def standardKeyToExecutor(standardKey : StandardKey) = {
    new StandardKeyExecutor(standardKey)
  }

  implicit def superColumnToExecutor(superColumn : SuperColumn) = {
    new SuperColumnExecutor(superColumn)
  }

  implicit def superKeyToExecutor(superKey : SuperKey) = {
    new SuperKeyExecutor(superKey)
  }

}