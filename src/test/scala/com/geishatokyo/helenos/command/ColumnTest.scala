package com.geishatokyo.helenos.command

import org.junit.runner.RunWith
import org.specs.Specification
import org.specs.runner.{JUnit, JUnitSuiteRunner}
import com.geishatokyo.helenos.column.{CFDefinition, KeyspaceDefinition}
import com.geishatokyo.helenos.conversions.StandardPreDefs._
import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.crudify.{BehaviorOnExist, Crudify}

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/11/27
 * Time: 22:01
 * To change this template use File | Settings | File Templates.
 */

@RunWith(classOf[JUnitSuiteRunner])
class ColumnTest extends Specification with JUnit  {

  val Keyspace = "KSForTest"
  val CF1 = "SetAndGetTestStandardCF"
  val CF2 = "SetAndGetTestSuperCF"

  val crudify = new Crudify(Session,behaviorOnExist = BehaviorOnExist.DropThenAdd)

  "set and get standard columns" should{
    doFirst{
      crudify.addColumnFamilies(CFDefinition.standardCF(Keyspace,CF1))
    }

    "set columns" in{
      Session.borrow(Keyspace)(s => {
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "int", 1 ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "long", 2L  ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "byte", 3.toByte ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "short", 4.toShort ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "float", 5.0f ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "double", 6.0 ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "string", "7" ).execute(s)
        new InsertColumn(Keyspace @@ CF1 \ "C1" \ "bool", true ).execute(s)
      })
    }

    "get not exist columns" in{
      Session.borrow(Keyspace)(session => {
        import session._
        new GetColumn(CF1 \ "C2" \ "int").execute(session) must_== None
      })

    }

    "get exist columns" in{
      Session.borrow(Keyspace)(s => {
        val c = new GetColumn(CF1 \ "C1" \ "int").execute(s).get
        val name : String = c.name
        name must_== "int"
        val i : Int = c.value
        i must_== 1
        val l : Long = new GetColumn(CF1 \ "C1" \ "long").execute(s).get.value
        l must_== 2L
        val b : Byte = new GetColumn(CF1 \ "C1" \ "byte").execute(s).get.value
        b must_== 3.toByte
        val shr : Short= new GetColumn(CF1 \ "C1" \ "short").execute(s).get.value
        shr must_== 4.toShort
        val f : Float = new GetColumn(CF1 \ "C1" \ "float").execute(s).get.value
        f must_== 5.0f
        val d : Double = new GetColumn(CF1 \ "C1" \ "double").execute(s).get.value
        d must_== 6.0
        val str : String = new GetColumn(CF1 \ "C1" \ "string").execute(s).get.value
        str must_== "7"
        val bool : Boolean = new GetColumn(CF1 \ "C1" \ "bool").execute(s).get.value
        bool must_== true
      })

    }

  }

  "set and get standard columns" should{
    doFirst{
      crudify.addColumnFamilies(CFDefinition.superCF(Keyspace,CF2))
    }

    "set columns" in{
      Session.borrow(Keyspace)(s => {
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "int", 1 ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "long", 2L  ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "byte", 3.toByte ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "short", 4.toShort ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "float", 5.0f ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "double", 6.0 ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "string", "7" ).execute(s)
        new InsertColumn(CF2 \\ "SC1" \ "C1" \ "bool", true ).execute(s)
      })
    }

    "get not exist columns" in{
      Session.borrow(Keyspace)(session => {
        import session._
        new GetColumn(CF2 \\ "SC1" \ "C2" \ "int").execute(session) must_== None
      })

    }

    "get exist columns" in{
      Session.borrow(Keyspace)(s => {
        val c = new GetColumn(CF2 \\ "SC1" \ "C1" \ "int").execute(s).get
        val name : String = c.name
        name must_== "int"
        val i : Int = c.value
        i must_== 1
        val l : Long = new GetColumn(CF2 \\ "SC1" \ "C1" \ "long").execute(s).get.value
        l must_== 2L
        val b : Byte = new GetColumn(CF2 \\ "SC1" \ "C1" \ "byte").execute(s).get.value
        b must_== 3.toByte
        val shr : Short= new GetColumn(CF2 \\ "SC1" \ "C1" \ "short").execute(s).get.value
        shr must_== 4.toShort
        val f : Float = new GetColumn(CF2 \\ "SC1" \ "C1" \ "float").execute(s).get.value
        f must_== 5.0f
        val d : Double = new GetColumn(CF2 \\ "SC1" \ "C1" \ "double").execute(s).get.value
        d must_== 6.0
        val str : String = new GetColumn(CF2 \\ "SC1" \ "C1" \ "string").execute(s).get.value
        str must_== "7"
        val bool : Boolean = new GetColumn(CF2 \\ "SC1" \ "C1" \ "bool").execute(s).get.value
        bool must_== true
      })

    }

  }

}