package com.geishatokyo.helenos.connection

import java.nio.ByteBuffer
import com.geishatokyo.helenos.{TimeUtil, CassandraException}
import java.util.ArrayList
import org.apache.cassandra.thrift.{SuperColumn => CSuperColumn, Column => CColumn, _}
import scala.collection.JavaConverters._
import com.geishatokyo.helenos.column.{ColumnForStandard,SuperColumn => HSuperColumn, _}
import com.geishatokyo.helenos.conversions.StandardPreDefs

/**
 * 
 * User: takeshita
 * Create: 11/11/06 19:56
 */
object StructureConversions extends StructureConversions{

}

/**
 * define conversions from helenos object to cassandra object and vice versa
 */
trait StructureConversions {

  implicit def arrayToBuffer(array : Array[Byte]) : ByteBuffer = {
    ByteBuffer.wrap(array)
  }

  implicit def toColumnPath( column : ColumnName) : ColumnPath = {
    val cp = new ColumnPath()
    cp.setColumn_family(column.columnFamily.name)
    cp.setColumn(column.name)
    if(column.isInstanceOf[ColumnNameForSuper]){
      cp.setSuper_column(column.asInstanceOf[ColumnNameForSuper].superColumn.name)
    }
    cp
  }

  implicit def toColumnParent( columnContainer : ColumnContainer) : ColumnParent = {
    val cp = new ColumnParent
    if(columnContainer.isInstanceOf[HSuperColumn]){
      cp.setSuper_column(columnContainer.asInstanceOf[HSuperColumn].name)
    }
    cp.setColumn_family(columnContainer.columnFamily.name)
    cp
  }
  implicit def toColumnParent( columnName : ColumnName) : ColumnParent = {
    val cp = new ColumnParent
    if(columnName.isInstanceOf[ColumnNameForSuper]){
      cp.setSuper_column(columnName.asInstanceOf[ColumnNameForSuper].superColumn.name)
    }
    cp.setColumn_family(columnName.columnFamily.name)
    cp
  }
  implicit def toColumnParent( superKey : SuperKey) : ColumnParent = {
    val cp = new ColumnParent
    cp.setColumn_family(superKey.columnFamily.name)
    cp
  }

  def toColumn( name : Array[Byte] , value : Array[Byte] ) : CColumn = {
    toColumn(name,value,TimeUtil.currentMicroSec)
  }
  def toColumn( name : Array[Byte] , value : Array[Byte] , timestamp : Long ) : CColumn = {
    val col = new CColumn
    col.setName(name)
    col.setValue(value)
    col.setTimestamp(timestamp)
    col
  }

  def toColumn( column : ColumnName , value : Array[Byte]) : CColumn = {
    toColumn(column.name,value , TimeUtil.currentMicroSec)
  }
  def toColumn( column : ColumnName , value : Array[Byte] , timestamp : Long) : CColumn = {
    toColumn(column.name,value,timestamp)
  }

  def toSuperColumn(superColumn : Array[Byte] , columnName : Array[Byte] , value : Array[Byte], timestamp : Long) : CSuperColumn = {
    val sc = new CSuperColumn
    sc.setName(superColumn : Array[Byte])
    val c = toColumn(columnName,value,timestamp)
    val l = new java.util.ArrayList[CColumn]()
    l.add(c)
    sc.setColumns(l)
    sc
  }

  def toSuperColumn(superColumn : HSuperColumn ,name : Array[Byte], value : Array[Byte]) : CSuperColumn = {
    toSuperColumn(superColumn.name,name,value,TimeUtil.currentMicroSec)
  }
  def toSuperColumn(column : ColumnNameForSuper , value : Array[Byte]) : CSuperColumn = {
    toSuperColumn(column.superColumn.name,column.name,value,TimeUtil.currentMicroSec)
  }

  def toCounterColumn( name : Array[Byte] , value : Long) : CounterColumn = {
    val cc = new CounterColumn()
    cc.setName(name)
    cc.setValue(value)
    cc
  }
  def toCounterColumn( column : ColumnName,  value : Long) : CounterColumn = {
    toCounterColumn(column.name,value)
  }

  def toSlicePredicate(predicate : Predicate) : SlicePredicate = {
    predicate match{
      case Columns( columns ) => {
        val sp = new SlicePredicate()
        sp.clear()
        sp.setColumn_names(columns.map(arrayToBuffer(_)).asJava)
        sp
      }
      case Range(start,finish,reversed) => {
        val sp = new SlicePredicate()
        val sr = new SliceRange
        sr.clear()
        sp.clear()
        sr.setStart(start)
        sr.setFinish(finish)
        //sr.setCount(1000000)
        sr.setReversed(reversed)
        sp.setSlice_range(sr)
        sp
      }
      case OffsetLimit(start,count,reversed) => {
        val sp = new SlicePredicate()
        val sr = new SliceRange
        sr.clear()
        sp.clear()
        sr.setStart(start)
        sr.setCount(count)
        sr.setReversed(reversed)
        sp.setSlice_range(sr)
        sr.setFinish(Array[Byte]())
        sp
      }
    }
  }


  implicit def columnOrSuperColumnToStandard( columnOrSuperColumn: ColumnOrSuperColumn) : ColumnForStandard = {

    if(columnOrSuperColumn.column != null){
      val col = columnOrSuperColumn.column
      new ColumnForStandard(col.getName,
        col.getValue,col.timestamp)
    }else if(columnOrSuperColumn.counter_column != null){
      val col = columnOrSuperColumn.counter_column
      new ColumnForStandard(col.getName,
        StandardPreDefs.bytes(col.getValue),0)
    }else{
      throw new CassandraException("Not standard column")
    }
  }

  implicit def columnOrSuperColumnToSuper( columnOrSuperColumn: ColumnOrSuperColumn) : ColumnForSuper = {
    if(columnOrSuperColumn.super_column != null){
      val superCol = columnOrSuperColumn.super_column
      val col = superCol.getColumns.get(0)
      new ColumnForSuper(superCol.getName,
        col.getName,
        col.getValue,
        col.timestamp)
    }else if(columnOrSuperColumn.counter_super_column != null){
      val superCol = columnOrSuperColumn.counter_super_column
      val col = superCol.getColumns().get(0)
      new ColumnForSuper(superCol.getName,
        col.getName,
        StandardPreDefs.bytes(col.getValue),
        0)
    }else if(columnOrSuperColumn.counter_column != null){
      // now super column's counter column returns counter_column.
      // TODO fix here after cassandra specification changes.
      val col = columnOrSuperColumn.counter_column
      new ColumnForSuper(Array[Byte](),
        col.getName,
        StandardPreDefs.bytes(col.getValue),
        0)
    }else{
      throw new CassandraException("Not super column")
    }
  }
  implicit def columnOrSuperColumnToSupers( columnOrSuperColumn: ColumnOrSuperColumn) : List[ColumnForSuper] = {
    if(columnOrSuperColumn.super_column != null){
    import scala.collection.JavaConversions._
      val superCol = columnOrSuperColumn.super_column
      superCol.getColumns map( col => {
        new ColumnForSuper(superCol.getName,
          col.getName,
          col.getValue,
          col.timestamp)
      }) toList
    }else{
      throw new CassandraException("Not super column")
    }
  }

}