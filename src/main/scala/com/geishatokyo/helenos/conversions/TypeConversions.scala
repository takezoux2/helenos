package com.geishatokyo.helenos.conversions

import com.geishatokyo.helenos.command._
import com.geishatokyo.helenos.executor.StandardColumnExecutor
import com.geishatokyo.helenos.column._
import java.util.{UUID, Date}

/**
 * 
 * User: takeshita
 * Create: 11/09/14 2:11
 */
object TypeConversions extends TypeConversions


/**
 * Define type conversion methods
 */
trait TypeConversions{


  def toArray( any : Any) : Array[Byte] = {
    any match{
      case s : String => bytes(s)
      case b : Array[Byte] => b
      case i : Int => bytes(i)
      case l : Long => bytes(l)
      case date : Date => bytes(date)
      case uuid : UUID => bytes(uuid)
      case f : Float => bytes(f)
      case d : Double => bytes(d)
      case _ => throw new Exception()
    }
  }


  implicit def bytes(date:Date):Array[Byte] = DateSerializer.toBytes(date)
  implicit def date(bytes:Array[Byte]):Date = DateSerializer.fromBytes(bytes)
  implicit def string(date:Date):String = DateSerializer.toString(date)

  implicit def bytes(b:Boolean):Array[Byte] = BooleanSerializer.toBytes(b)
  implicit def boolean(bytes:Array[Byte]):Boolean = BooleanSerializer.fromBytes(bytes)
  implicit def string(b:Boolean):String = BooleanSerializer.toString(b)


  implicit def bytes(b:Float):Array[Byte] = FloatSerializer.toBytes(b)
  implicit def float(bytes:Array[Byte]):Float = FloatSerializer.fromBytes(bytes)
  implicit def string(b:Float):String = FloatSerializer.toString(b)

  implicit def bytes(b:Double):Array[Byte] = DoubleSerializer.toBytes(b)
  implicit def double(bytes:Array[Byte]):Double = DoubleSerializer.fromBytes(bytes)
  implicit def string(b:Double):String = DoubleSerializer.toString(b)

  implicit def bytes(l:Long):Array[Byte] = LongSerializer.toBytes(l)
  implicit def long(bytes:Array[Byte]):Long = LongSerializer.fromBytes(bytes)
  implicit def string(l:Long):String = LongSerializer.toString(l)

  implicit def bytes(i:Int):Array[Byte] = IntSerializer.toBytes(i)
  implicit def int(bytes:Array[Byte]):Int = IntSerializer.fromBytes(bytes)
  implicit def string(i:Int) = IntSerializer.toString(i)

  implicit def bytes(l:Short):Array[Byte] = FreeIntSerializer.toBytes(l)
  implicit def short(bytes:Array[Byte]) : Short = FreeIntSerializer.fromBytes(bytes).toShort
  implicit def string(l:Short):String = FreeIntSerializer.toString(l)

  implicit def bytes(l:Byte):Array[Byte] = FreeIntSerializer.toBytes(l)
  implicit def byte(bytes:Array[Byte]) : Byte = FreeIntSerializer.fromBytes(bytes).toByte
  implicit def string(l:Byte):String = FreeIntSerializer.toString(l)

  implicit def bytes(str:String):Array[Byte] = StringSerializer.toBytes(str)
  implicit def string(bytes:Array[Byte]):String = StringSerializer.fromBytes(bytes)

  implicit def string(source:UUID) = UUIDSerializer.toString(source)
  implicit def uuid(source:String) = UUIDSerializer.fromString(source)
  implicit def bytes(source:UUID):Array[Byte] = UUIDSerializer.toBytes(source)


  // for structures

  implicit def strToKeyspace(name : String) = new Keyspace(name)
  implicit def strToColumnFamily(name : String) = new ColumnFamily(name)

  implicit def bytesToMutator(name : Array[Byte]) = new Mutator(name)
  implicit def strToMutator(name : String) = new Mutator(bytes(name))




}

class Mutator( name : Array[Byte]){

  def :=( value : Array[Byte]) = {
    Insertion(name,value)
  }

  def del = {Deletion(name)}

  def +=( value : Long) = Increment(name,value)
  def -=(value : Long) = this.+=(-value)


}

