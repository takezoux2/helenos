package com.geishatokyo.helenos.conversions

import com.geishatokyo.helenos.column._
import com.geishatokyo.helenos.executor._
import com.geishatokyo.helenos.connection.SessionPool

/**
 * 
 * User: takeshita
 * Create: 11/11/11 1:18
 */

trait ExecutorConversions {

  def sessionPool : SessionPool

  //for command

  implicit def columnToExecutor( column : ColumnNameForStandard) = {
    new StandardColumnExecutor(column)(sessionPool)
  }
  implicit def columnForSuperToExecutor( column : ColumnNameForSuper) = {
    new ColumnForSuperExecutor(column)(sessionPool)
  }

  implicit def standardKeyToExecutor(standardKey : StandardKey) = {
    new StandardKeyExecutor(standardKey)(sessionPool)
  }

  implicit def superColumnToExecutor(superColumn : SuperColumn) = {
    new SuperColumnExecutor(superColumn)(sessionPool)
  }

  implicit def superKeyToExecutor(superKey : SuperKey) = {
    new SuperKeyExecutor(superKey)(sessionPool)
  }

}