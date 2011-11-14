package com.geishatokyo.helenos

import column._
import command._
import connection.{SimpleConnectionPool, OneTimeSessionPool, Session}
import crudify.{BehaviorOnNotExist, BehaviorOnExist, Crudify}
import org.junit.runner.RunWith
import org.specs.Specification
import org.specs.runner.{JUnit, JUnitSuiteRunner}
import org.apache.cassandra.thrift.IndexType

/**
 * 
 * User: takeshita
 * Create: 11/11/09 13:05
 */

@RunWith(classOf[JUnitSuiteRunner])
class CrudifyTest extends Specification with JUnit {


  val keyspaceNameForTest = "KeyspaceForCrudifyTest"

  val KeyspaceForDescExist = "KeyspaceForTestExsit"
  val KeyspaceForDescNotExist = "KeyspaceForTestNotExsit"
  val KeyspaceForCrudify = "KeyspaceForCrudify"

  val ColumnFamilyTest1 = "ColumnFamilyTest1"

  doBeforeSpec{
    SessionInitializer.init()
  }

  "keyspace" should{
    doFirst{
      Session.systemBorrow(session => {
        import session._
        new DropKeyspace(keyspaceNameForTest).execute
        new AddKeyspace(KeyspaceForDescExist).execute
        new DropKeyspace(KeyspaceForDescNotExist).execute
        new DropKeyspace(KeyspaceForCrudify).execute
        new DropKeyspace(ColumnFamilyTest1).execute
      })
      Thread.sleep(1000)
    }


     "add" in{

       Session.systemBorrow(session => {
         import session._
         new AddKeyspace(keyspaceNameForTest).execute must_== AddResult.Add
         new AddKeyspace(keyspaceNameForTest).execute must_== AddResult.Nothing
       })

     }
  }

  "column family" should{

    doFirst{
      Session.systemBorrow(session => {
        new AddKeyspace(ColumnFamilyTest1).execute(session)
      })
    }

    "add standard column" in{
      Session.borrow(ColumnFamilyTest1)(session => {
        import session._
        new AddColumnFamily(CFDefinition.standardCF(ColumnFamilyTest1,"Standard1")).execute must_== AddResult.Add
        new AddColumnFamily(CFDefinition.standardCF(ColumnFamilyTest1,"Standard1")).execute must_== AddResult.Nothing
      })


    }

    def addCF(name : String, comparator : String) = {
      Session.borrow(ColumnFamilyTest1)(session => {
        new AddColumnFamily(CFDefinition.standardCF(ColumnFamilyTest1,name).withComparator(comparator)).execute(session) must_==  AddResult.Add
      })
    }

    "add variable standard columns" in{
      addCF("STByte",ComparatorType.BytesType)
      addCF("STUTF8",ComparatorType.UTF8Type)
      addCF("STLong",ComparatorType.LongType)
      addCF("STLexicalUUID",ComparatorType.LexicalUUIDType)
      addCF("STTImeUUID",ComparatorType.TimeUUIDType)
      addCF("STAscii",ComparatorType.AsciiType)
    }

    "add super column" in{
      Session.borrow(ColumnFamilyTest1)(session => {
        import session._
        new AddColumnFamily(CFDefinition.superCF(ColumnFamilyTest1,"SuperCF1")).execute must_== AddResult.Add
      })
    }

    "add standard column with index" in{
      Session.borrow(ColumnFamilyTest1)(session => {
        import session._
        new AddColumnFamily(CFDefinition.standardCF(ColumnFamilyTest1,"Standard2") :+= ColumnDefinition.index("index1")).execute must_== AddResult.Add
      })

    }

  }


  "crudify" should{
    doFirst{
      Session.systemBorrow(session => {
        new DropKeyspace(KSForCRUDCF1).execute(session)
        new DropKeyspace(KSForCRUDCF2).execute(session)
        new DropKeyspace(KeyspaceForCrudify).execute(session)
        new AddKeyspace(new KeyspaceDefinition(KSForCRUDCF2)).execute(session)
      })
    }

    "drop and create keyspaces" in{
      val crudify = new Crudify(Session,BehaviorOnExist.Update)

      crudify.addKeyspaces(new KeyspaceDefinition(KeyspaceForCrudify)) must_== Map(KeyspaceForCrudify -> AddResult.Add)
      crudify.addKeyspaces(new KeyspaceDefinition(KeyspaceForCrudify)) must_== Map(KeyspaceForCrudify ->AddResult.Nothing)
      val ks = new KeyspaceDefinition(KeyspaceForCrudify)
      ks.ksDef.setDurable_writes(false)
      crudify.addKeyspaces(ks) must_== Map(KeyspaceForCrudify ->AddResult.Update)
    }
    val KSForCRUDCF1 = "KSForCRUDCF1"
    val KSForCRUDCF2 = "KSForCRUDCF2"

    "drop and create column families" in{
      println("#####")


      val crudify = new Crudify(Session,BehaviorOnExist.Nothing,BehaviorOnNotExist.Create)

      crudify.addColumnFamilies(
        CFDefinition.standardCF(KSForCRUDCF1,"Standard1"),
        CFDefinition.superCF(KSForCRUDCF1,"Super1"),
        CFDefinition.standardCF(KSForCRUDCF2,"Standard2"),
        CFDefinition.superCF(KSForCRUDCF2,"Super2") ) foreach( v => {
        println( v._1 +  " -> " + v._2)
        v._2 must_== AddResult.Add
      })

      crudify.addColumnFamilies(CFDefinition.superCF(KSForCRUDCF1,"Standard1")) must_== Map( "Standard1" -> AddResult.Nothing)

    }
  }



}