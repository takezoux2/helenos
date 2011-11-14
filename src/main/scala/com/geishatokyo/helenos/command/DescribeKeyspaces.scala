package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import scala.collection.JavaConverters._
import org.apache.cassandra.thrift.{NotFoundException, KsDef}
import com.geishatokyo.helenos.column.KeyspaceDefinition

/**
 * 
 * User: takeshita
 * Create: 11/11/09 11:57
 */

class DescribeKeyspaces extends SystemCommand[List[KeyspaceDefinition]]{
  def execute(implicit session : Session) = {
    session().describe_keyspaces().asScala.toList.map(new KeyspaceDefinition(_))
  }
}

class DescribeKeyspace(keyspace : String) extends SystemCommand[KeyspaceDefinition]{
  def execute(implicit session : Session) = {

    try{
      val ks = session().describe_keyspace(keyspace)
      new KeyspaceDefinition(ks)
    }catch{
      case e : NotFoundException => {
        logger.info("Keyspace:%s is not found".format(keyspace))
        null
      }
    }
  }
}