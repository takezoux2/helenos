package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import org.apache.cassandra.thrift.ConsistencyLevel
import scala.collection.JavaConversions._
import com.geishatokyo.helenos.column._



/**
 * 
 * User: takeshita
 * Create: 11/11/11 18:29
 */

class GetStandardSlice( standardKey : StandardKey , predicate : Predicate) extends Command[List[ColumnForStandard]] {
  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    val results = session().get_slice(standardKey.key,
    toColumnParent(standardKey),
    toSlicePredicate(predicate),
    consistencyLevel)

    if(results == null) Nil
    else{
      results.map(c => {
        columnOrSuperColumnToStandard(c)
      }).toList
    }
  }
}

class GetSuperSlice( superColumn : SuperColumn , predicate : Predicate) extends Command[List[ColumnForSuper]] {
  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    val results = session().get_slice(superColumn.key.key,
    toColumnParent(superColumn),
    toSlicePredicate(predicate),
    consistencyLevel)

    if(results == null) Nil
    else{
      results.map(c => {
        columnOrSuperColumnToSuper(c)
      }).toList
    }
  }
}
class GetSuperColumnsSlice( superKey : SuperKey , predicate : Predicate) extends Command[Map[Array[Byte],List[ColumnForSuper]]] {
  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {
    val results = session().get_slice(superKey.key,
    toColumnParent(superKey),
    toSlicePredicate(predicate),
    consistencyLevel)

    import scala.collection.JavaConversions._

    if(results == null) Map()
    else{
      Map(results.map( cos => {
        val cols = columnOrSuperColumnToSupers(cos)
        if(cols.size > 0){
          cols(0).superColumnName -> cols.toList
        }else{
          new Array[Byte](0) -> Nil
        }
      }) :_*)
    }
  }
}