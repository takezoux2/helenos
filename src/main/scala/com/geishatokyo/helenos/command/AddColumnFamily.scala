package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import org.apache.cassandra.thrift.InvalidRequestException
import com.geishatokyo.helenos.column.{KeyspaceDefinition, CFDefinition}

/**
 * 
 * User: takeshita
 * Create: 11/11/10 0:16
 */

class AddColumnFamily(cfDef :CFDefinition) extends SystemCommand[AddResult.Value]{
  def execute(implicit session: Session) : AddResult.Value = {
    logger.execute("Add column family",cfDef.name)
    try{
      session().system_add_column_family(cfDef.cfDef)
      AddResult.Add
    }catch{
      case e : InvalidRequestException => {
        if(e.getMessage == null){
          val ks = new DescribeKeyspace((cfDef.keyspace)).execute(session)
          if(ks.indexOf(cfDef.name) >= 0){
            return AddResult.Nothing
          }else{
            throw e
          }
        }else{
          throw e
        }
      }
      case e : Exception => throw e
    }
  }
}
class UpdateColumnFamily(cfDef :CFDefinition) extends SystemCommand[AddResult.Value]{
  def execute(implicit session: Session) = {
    logger.execute("Update column family",cfDef.name)
    try{
      session().system_update_column_family(cfDef.cfDef)
      AddResult.Update
    }catch{
      case e : InvalidRequestException => {
        if(e.getMessage == null){
          val ks = new DescribeKeyspace((cfDef.keyspace)).execute(session)
          if(ks.indexOf(cfDef.name) >= 0){
            throw e
          }else{
            AddResult.Nothing
          }
        }else{
          throw e
        }
      }
      case e : Exception => throw e
    }
  }
}

class DropColumnFamily(keyspace : String,  columnFamilyName : String) extends SystemCommand[AddResult.Value]{


  def execute(implicit session: Session) : AddResult.Value = {
    logger.execute("Drop column family",columnFamilyName)
    try{
      session().system_drop_column_family(columnFamilyName)
      AddResult.Drop

    }catch{
      case e : InvalidRequestException => {
        if(e.getMessage == null){
          val ks = new DescribeKeyspace(keyspace).execute(session)
          if(ks.indexOf(columnFamilyName) >= 0){
            throw e
          }else{
            AddResult.Nothing
          }
        }else{
          throw e
        }
      }
      case e : Exception => throw e
    }
  }
}