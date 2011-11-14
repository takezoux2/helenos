package com.geishatokyo.helenos

import column._
import command.{DescribeKeyspace, AddResult}
import connection.Session
import conversions.StandardPreDefs
import org.junit.runner.RunWith
import org.specs._
import mock.JMocker
import org.specs.runner.{ JUnitSuiteRunner, JUnit }

import StandardPreDefs._
import org.apache.cassandra.thrift.ConsistencyLevel
import com.geishatokyo.helenos.crudify.Crudify

@RunWith(classOf[JUnitSuiteRunner])
class UsageTest extends Specification with JUnit{

  val Keyspace = "UsageTest"

  doBeforeSpec{
    SessionInitializer.init(Keyspace)

    val crudify = new Crudify(Session)
    crudify.dropKeyspaces(Keyspace)
    crudify.addColumnFamilies(
      CFDefinition.typicalTextStandardCF("UsageTest","StandardCF") :+= ColumnDefinition.counter("age"),
      CFDefinition.typicalStandardCounterCF("UsageTest","CounterCF"),
      CFDefinition.superCF("UsageTest","SuperCF"),
      CFDefinition.typicalSuperCounterCF("UsageTest","SuperCounterCF")
    )

    Thread.sleep(1000)
  }

  "helenos" should{
    "access single column" in{

      "StandardCF" \ "Tom" \ "name" := "Tom"

      val a = "StandardCF" \ "Tom" \ "name" get

      "StandardCF" \ "Tom" \ "name" := "Tom"
      "StandardCF" \ "Tom" \ "name" del

      "StandardCF" \ "Tom" := List("name" := "Tom",
                                      "gender" := 2,
                                      "birthday" del)
      var l : Long = "CounterCF" \ "tom" \ "age" counter

      println("#" + l)

      "CounterCF" \ "tom" \ "age" += 2
      "CounterCF" \ "tom" \ "age" -= 2
      "CounterCF" \ "tom" \ "age" resetCounter

      l  = "CounterCF" \ "tom" \ "age" counter

      println("Counter hogehoge = " + ("CounterCF" \ "tom" \ "hogehoge" counter))

      println("#" + l)
      "SuperCF" \\ "2011" \ "Apple" \ "provide" := "aomori"
      "SuperCF" \\ "2011" \ "Apple" \ "provide" del

      "SuperCF" \\ "2011" \ "Apple" := List("provide" := "Tom",
                                      "count" := 2,
                                      "taken" del)

      "SuperCounterCF" \\ "2011" \ "Apple" \ "count" += 2
      "SuperCounterCF" \\ "2011" \ "Apple" \ "count" counter

      "SuperCounterCF" \\ "2011" \ "Apple" \ "count" resetCounter

      "SuperCounterCF" \\ "2011" \ "Apple" \ "count" counter

      "SuperCounterCF" \\ "2011" \ "Apple" \ "hogehoge" counter
    }

    "access multi columns" in{
      {
      val getColumns : Map[Array[Byte],Array[Byte]] = "StandardCF" \ "Tom" get // this is same as limit(100) if you want to read more, please call limit

      val getSlice : Map[Array[Byte],Array[Byte]] = "StandardCF" \ "Tom" slice("name","age")
      val getRange : Map[Array[Byte],Array[Byte]] = "StandardCF" \ "Tom" range("name5","name4",reversed = true)
      val getLimit : Map[Array[Byte],Array[Byte]] = "StandardCF" \ "Tom" limit(start="name1",limit=100,reversed=true)
      }


      {
      val getColumns : Map[Array[Byte],Array[Byte]] = "SuperCF" \\ "Apple" \ "2011" get // this is same as limit(100) if you want to read more, please call limit

      val getSlice : Map[Array[Byte],Array[Byte]] = "SuperCF" \\ "Apple" \ "2011" slice("provide","taken")
      val getRange : Map[Array[Byte],Array[Byte]] = "SuperCF" \\ "Apple" \ "2011" range("name5","name4",reversed = true)
      val getLimit : Map[Array[Byte],Array[Byte]] = "SuperCF" \\ "Apple" \ "2011" limit(start="name1",limit=100,reversed=true)

      }

      //val getMultiKeysAndColumns : Map[Array[Byte],Map[Array[Byte],Array[Byte]]] = "StandardCF" \ List("Tom","Bob") slice("name","age")
    }

    "access super columns" in{
      val getSuperColumns : Map[Array[Byte],Map[Array[Byte],Array[Byte]]] = "SuperCF" \\ "Apple" get

    }
  }

}