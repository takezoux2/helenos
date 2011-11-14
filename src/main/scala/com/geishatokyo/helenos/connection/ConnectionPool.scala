package com.geishatokyo.helenos.connection

import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}

/**
 * 
 * User: takeshita
 * Create: 11/09/14 12:23
 */
class SimpleConnectionPool(host : String = "localhost", port : Int = 9160) extends ConnectionPool{


  def getClient = {
    val tr = new TFramedTransport(new TSocket("localhost",9160,0))
    //val tr = new TSocket(host,port,0)
    val protocol = new TBinaryProtocol(tr)
    val client = new Cassandra.Client(protocol)
    tr.open()
    client
  }
  def returnClient(client: Cassandra.Iface) = {
    try{
      println("close client")
      client match{
        case c : Cassandra.Client => {
          c.getInputProtocol().getTransport.close()
          c.getOutputProtocol().getTransport.close()
        }
        case _ =>
      }
    }catch{
      case e: Exception => {
        e.printStackTrace
      }
    }
  }

}

trait ConnectionPool{


  def getClient : Cassandra.Iface

  def returnClient(client : Cassandra.Iface)

}