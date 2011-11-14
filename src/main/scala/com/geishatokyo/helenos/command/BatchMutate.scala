package com.geishatokyo.helenos.command

import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.TimeUtil
import org.apache.cassandra.thrift.{SlicePredicate, ColumnOrSuperColumn, Mutation => CMutation, Deletion => CDeletion, ConsistencyLevel}
import com.geishatokyo.helenos.column._
import java.util.{HashMap, ArrayList}
import scala.collection.JavaConverters._
import java.nio.ByteBuffer

/**
 * 
 * User: takeshita
 * Create: 11/11/11 0:53
 */

class BatchMutateStandard( standardKey : StandardKey, mutations: List[Mutation]) extends Command[Boolean]{


  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {

    val map = new HashMap[ByteBuffer, java.util.Map[String, java.util.List[CMutation]]]()
    val timestamp = TimeUtil.currentMicroSec

    val cMutations = mutations.map( _ match{
      case Insertion(name , value) => {
        val cm = new CMutation()
        val cOrS = new ColumnOrSuperColumn
        cOrS.setColumn(toColumn(name,value))
        cm.setColumn_or_supercolumn(cOrS)
        cm
      }
      case Increment(name , value) => {
        val cm = new CMutation()
        val cOrS = new ColumnOrSuperColumn
        cOrS.setCounter_column(toCounterColumn(name,value))
        cm.setColumn_or_supercolumn(cOrS)
        cm
      }
      case Deletion(name) => {
        val sp = new SlicePredicate
        val columnNames = new ArrayList[ByteBuffer]()
        columnNames.add(name)
        sp.setColumn_names(columnNames)

        val d = new CDeletion
        d.setTimestamp(timestamp)
        d.setPredicate(sp)

        val cm = new CMutation()
        cm.setDeletion(d)
        cm
      }
    })

    val innerMap = new HashMap[String, java.util.List[CMutation]]()
    innerMap.put(standardKey.columnFamily.name, cMutations.asJava)
    map.put(standardKey.key, innerMap)
    session().batch_mutate(map,consistencyLevel)


    true


  }
}
class BatchMutateSuper( superColumn : SuperColumn, mutations: List[Mutation]) extends Command[Boolean]{


  def execute(session: Session, consistencyLevel: ConsistencyLevel) = {

    val map = new HashMap[ByteBuffer, java.util.Map[String, java.util.List[CMutation]]]()
    val timestamp = TimeUtil.currentMicroSec

    val cMutations = mutations.map( _ match{
      case Insertion(name , value) => {
        val cm = new CMutation()
        val cOrS = new ColumnOrSuperColumn
        cOrS.setSuper_column(toSuperColumn(superColumn,name,value))
        cm.setColumn_or_supercolumn(cOrS)
        cm
      }
      case Increment(name , value) => {
        val cm = new CMutation()
        val cOrS = new ColumnOrSuperColumn
        cOrS.setCounter_column(toCounterColumn(name,value))
        cm.setColumn_or_supercolumn(cOrS)
        cm
      }
      case Deletion(name) => {
        val sp = new SlicePredicate
        val columnNames = new ArrayList[ByteBuffer]()
        columnNames.add(name)
        sp.setColumn_names(columnNames)

        val d = new CDeletion
        d.setTimestamp(timestamp)
        d.setPredicate(sp)
        d.setSuper_column(superColumn.name)

        val cm = new CMutation()
        cm.setDeletion(d)
        cm
      }
    })

    val innerMap = new HashMap[String, java.util.List[CMutation]]()
    innerMap.put(superColumn.columnFamily.name, cMutations.asJava)
    map.put(superColumn.key.key, innerMap)
    session().batch_mutate(map,consistencyLevel)


    true


  }
}