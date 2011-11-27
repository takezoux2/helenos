package com.geishatokyo.helenos.command

import org.specs.Specification
import org.junit.runner.RunWith
import org.specs.runner.{JUnitSuiteRunner, JUnit}
import com.geishatokyo.helenos.SessionInitializer
import com.geishatokyo.helenos.crudify.Crudify
import com.geishatokyo.helenos.column.KeyspaceDefinition
import com.geishatokyo.helenos.connection.Session
import org.apache.cassandra.thrift.InvalidRequestException

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/11/27
 * Time: 21:39
 * To change this template use File | Settings | File Templates.
 */

@RunWith(classOf[JUnitSuiteRunner])
class SetKeyspaceTest extends Specification with JUnit {

  doFirst{
    SessionInitializer.init()
  }

  "Set keyspace" should{

    doFirst{
      Crudify.addKeyspaces(new KeyspaceDefinition("TestForSetKeyspace"))
      Crudify.dropKeyspaces("TestNotExistKeyspace")
    }

    "set exist keyspace" in {
      Session.systemBorrow(session => {
        new SetKeyspace("TestForSetKeyspace").execute(session)
      })
      true must_== true
    }

    "set not exist keyspace" in{
      Session.systemBorrow(session => {
        new SetKeyspace("TestNotExistKeyspace").execute(session) must throwA[InvalidRequestException]
      })
    }

  }

}