package com.geishatokyo.helenos

import command.DescribeKeyspace
import connection.{SimpleConnectionPool, OneTimeSessionPool, Session, ConnectionPool}
import conversions.StandardPreDefs
import org.junit.runner.RunWith
import org.specs.Specification
import org.specs.runner.{JUnit, JUnitSuiteRunner}
import org.apache.thrift.protocol.TBinaryProtocol
import conversions.StandardPreDefs._
import org.apache.thrift.transport.{TFramedTransport, TTransport, TSocket}
import org.apache.cassandra.thrift.Cassandra.Client
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{ConsistencyLevel, ColumnPath, Cassandra}

/**
 * 
 * User: takeshita
 * Create: 11/09/19 13:53
 */

@RunWith(classOf[JUnitSuiteRunner])
class ReadTest extends Specification with JUnit{


  doBeforeSpec{
    SessionInitializer.init()
  }

  import StandardPreDefs._

  "insert" should{


    "insert" in {



      val col = "Standard1" \ "Tom" \ "name" get

      println("###" + col)

      "Standard1" \ "Tom" \ "name" := "Tom"
    }
  }

}