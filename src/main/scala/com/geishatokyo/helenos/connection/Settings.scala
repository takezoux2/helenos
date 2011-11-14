package com.geishatokyo.helenos.connection

import org.apache.cassandra.thrift.ConsistencyLevel

/**
 * 
 * User: takeshita
 * Create: 11/09/19 15:38
 */

object Settings{


  var defaultReadConsistency = {
    ConsistencyLevel.QUORUM
  }

  var defaultWriteConsistency = {
    ConsistencyLevel.QUORUM
  }

}