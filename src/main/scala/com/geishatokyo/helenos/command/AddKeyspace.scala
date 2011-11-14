package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import java.util.ArrayList
import com.geishatokyo.helenos.column.KeyspaceDefinition
import org.apache.cassandra.thrift.{InvalidRequestException, CfDef, KsDef}

/**
 * 
 * User: takeshita
 * Create: 11/11/06 21:46
 */

class AddKeyspace(val ksDef : KeyspaceDefinition) extends SystemCommand[AddResult.Value]{

  def this(name : String) = {
    this(new KeyspaceDefinition(name))
  }

  /**
   * Create new keyspace
   * if keyspace already exists, this method returns null
   * @param sesssion seeesion
   * @return KeyspaceUUID or null)
   */
  def execute(implicit session : Session) = {
    logger.execute("Add keyspace" ,ksDef.name)
    try{
      val uuid = session().system_add_keyspace(ksDef.ksDef)
      logger.info("Done adding keyspace:%s uuid:%s".format(ksDef.name,uuid))
      AddResult.Add
    }catch{
      case e : InvalidRequestException => {
        if(e.getMessage == null){
          if(new DescribeKeyspace(ksDef.name).execute != null ){
            logger.warn("keyspace:%s already exists".format(ksDef.name))
            AddResult.Nothing
          }else{
            throw e
          }
        }else{
          throw e
        }
      }
    }
  }
}

class DropKeyspace( keyspace : String) extends SystemCommand[AddResult.Value]{
  def execute(implicit session : Session) : AddResult.Value = {
    logger.execute("Drop keyspace", keyspace)
    try{
      logger.debug("Drop reulst:" +  session().system_drop_keyspace(keyspace))
      AddResult.Drop
    }catch{
      case e : InvalidRequestException => {
        if(e.getMessage == null){
          if(new DescribeKeyspace(keyspace).execute != null ){
            throw e
          }else{
            AddResult.Nothing
          }
        }else{
          throw e
        }
      }
    }
  }
}

class UpdateKeyspace(val ksDef : KeyspaceDefinition) extends SystemCommand[AddResult.Value]{
  def execute(implicit session: Session) = {
    logger.execute("Update keyspace", ksDef.name)
    session().system_update_keyspace(ksDef.ksDef)
    AddResult.Update
  }
}