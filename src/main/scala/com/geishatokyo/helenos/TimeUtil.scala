package com.geishatokyo.helenos

import java.util.Date

/**
 * Util for time
 * User: takeshita
 * Create: 11/09/14 2:36
 */

object TimeUtil{

  /**
   * get current micro time.
   */
  def currentMicroSec : Long = {
    new Date().getTime * 1000
  }

}