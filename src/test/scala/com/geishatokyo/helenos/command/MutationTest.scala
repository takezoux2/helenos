package com.geishatokyo.helenos.command

import org.junit.runner.RunWith
import org.specs.Specification
import org.specs.runner.{JUnit, JUnitSuiteRunner}
import com.geishatokyo.helenos.column.CFDefinition
import com.geishatokyo.helenos.conversions.StandardPreDefs._
import com.geishatokyo.helenos.connection.Session
import com.geishatokyo.helenos.crudify.{BehaviorOnExist, Crudify}
import com.geishatokyo.helenos.SessionInitializer

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/11/27
 * Time: 22:01
 * To change this template use File | Settings | File Templates.
 */

@RunWith(classOf[JUnitSuiteRunner])
class MutationTest extends Specification with JUnit  {

  val Keyspace = "KSForTest"
  val CF1 = "MutationStandardCF"
  doBeforeSpec{
    SessionInitializer.init(Keyspace)
  }


  def crudify = new Crudify(Session,behaviorOnExist = BehaviorOnExist.DropThenAdd)

  "standard mutation" should{
    doFirst{
      crudify.addColumnFamilies(CFDefinition.standardCF(Keyspace,CF1))
    }

    "inserts" in{
      Session.borrow(Keyspace)(s => {
        new BatchMutateStandard(CF1 \ "m1",List("int" := 2,"str" := "hoge")).execute(s)

        (CF1 \ "m1" \ "int" get : Int) must_== 2
        (CF1 \ "m1" \ "str" get : String) must_== "hoge"

      })
    }

    "deletes" in{
      Session.borrow(Keyspace)(s => {
        new BatchMutateStandard(CF1 \ "m1",List("str" del,"fuga" del)).execute(s)

        (CF1 \ "m1" \ "str" getOp) must_== None
        (CF1 \ "m1" \ "fuga" getOp) must_== None

      })

    }

    "inserts and deletes" in{
      Session.borrow(Keyspace)(s => {
        new BatchMutateStandard(CF1 \ "m2",List("int" := 23,"str" del,"fuga" del)).execute(s)

        (CF1 \ "m2" \ "int" get : Int) must_== 23
        (CF1 \ "m1" \ "str" getOp) must_== None

      })

    }


  }

}