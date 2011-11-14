package com.geishatokyo.helenos.command

import org.slf4j.LoggerFactory

/**
 * 
 * User: takeshita
 * Create: 11/11/09 12:47
 */

object CommandLogger {

  val logger = LoggerFactory.getLogger(getClass)

  def execute( commandName : String , message : String , formatArgs : Any*) = {
    if(logger.isDebugEnabled){
      logger.debug("[%s] %s".format(commandName ,message.format(formatArgs)))
    }
  }
  def debug(message : String , formatArgs : Any*) = {
    logger.debug(message.format(formatArgs))
  }
  def info(message : String , formatArgs : Any*) = {
    logger.info(message.format(formatArgs))
  }
  def warn(message : String , formatArgs : Any*) = {
    logger.warn(message.format(formatArgs))
  }
  def error(message : String , formatArgs : Any*) = {
    logger.error(message.format(formatArgs))
  }
  def warn(e : Throwable,message : String , formatArgs : Any*) = {
    logger.warn(message.format(formatArgs),e)
  }
  def error(e : Throwable,message : String , formatArgs : Any*) = {
    logger.error(message.format(formatArgs),e)
  }


}