package com.geishatokyo.helenos.conversions

import java.nio.charset.Charset
import java.nio.ByteBuffer
import java.util.{UUID, Date}

object Serializer {

  /**
   * defines a map of all the default serializers
   */
  val Default = Map[Class[_], Serializer[_]](
    (classOf[String]  -> StringSerializer),
    (classOf[UUID]    -> UUIDSerializer),
    (classOf[Int]     -> IntSerializer),
    (classOf[Long]    -> LongSerializer),
    (classOf[Boolean] -> BooleanSerializer),
    (classOf[Float]   -> FloatSerializer),
    (classOf[Double]  -> DoubleSerializer),
    (classOf[Date]    -> DateSerializer)
  )
}

/**
 *  defines a class responsible for converting an object to and from an
 * array of bytes.
 *
 * @author Chris Shorrock
 */
trait Serializer[A] {
  /** converts this object to a byte array for entry into cassandra */
  def toBytes(obj:A):Array[Byte]

  /** converts the specified byte array into an object */
  def fromBytes(bytes:Array[Byte]):A

  /** converts the specified value to a string */
  def toString(obj:A):String

  /** converts the specified value from a string */
  def fromString(str:String):A
}

object StringSerializer extends Serializer[String] {
  val utf8 = Charset.forName("UTF-8")

  def toBytes(str:String) = str.getBytes(utf8)
  def fromBytes(bytes:Array[Byte]) = new String(bytes, utf8)
  def toString(str:String) = str
  def fromString(str:String) = str
}

object UUIDSerializer extends Serializer[UUID] {
  def fromBytes(bytes:Array[Byte]) = {
    new UUID(LongSerializer.fromBytes(bytes.slice(0,8)),
             LongSerializer.fromBytes(bytes.slice(8,16)))
  }
  def toString(uuid:UUID) = uuid.toString
  def fromString(str:String) = UUID.fromString(str)

  def toBytes(uuid:UUID) = {
    val msb = uuid.getMostSignificantBits()
    val lsb = uuid.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    (0 until 8).foreach  { (i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte] }
    (8 until 16).foreach { (i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte] }

    buffer
  }

}

object FreeIntSerializer extends Serializer[Long] {
  val bytesPerInt = java.lang.Integer.SIZE / java.lang.Byte.SIZE

  def toBytes(i:Long) = {
    if(Integer.MIN_VALUE < i && i < Integer.MAX_VALUE){
      ByteBuffer.wrap(new Array[Byte](bytesPerInt)).putInt(i.toInt).array()
    }else{
      ByteBuffer.wrap(new Array[Byte](bytesPerInt * 2)).putLong(i).array()
    }
  }
  def fromBytes(bytes:Array[Byte]) : Long = {
    bytes.length match{
      case 4 | 3 => ByteBuffer.wrap(bytes).getInt
      case i if i > 4 && i <= 8 => ByteBuffer.wrap(bytes).getLong
      case 1 => bytes(0)
      case 2 => ByteBuffer.wrap(bytes).getShort
      case 0 => 0
      case _ => ByteBuffer.wrap(bytes).getLong
    }
  }
  def toString(obj:Long) = obj.toString
  def fromString(str:String) = str.toLong
}

object IntSerializer extends Serializer[Int] {
  val bytesPerInt = java.lang.Integer.SIZE / java.lang.Byte.SIZE

  def toBytes(i:Int) = ByteBuffer.wrap(new Array[Byte](bytesPerInt)).putInt(i).array()
  def fromBytes(bytes:Array[Byte]) = ByteBuffer.wrap(bytes).getInt()
  def toString(obj:Int) = obj.toString
  def fromString(str:String) = str.toInt
}

object LongSerializer extends Serializer[Long] {
  val bytesPerLong = java.lang.Long.SIZE / java.lang.Byte.SIZE

  def toBytes(l:Long) = ByteBuffer.wrap(new Array[Byte](bytesPerLong)).putLong(l).array()
  def fromBytes(bytes:Array[Byte]) = ByteBuffer.wrap(bytes).getLong()
  def toString(obj:Long) = obj.toString
  def fromString(str:String) = str.toLong
}

object BooleanSerializer extends Serializer[Boolean] {
  def toBytes(b:Boolean) = StringSerializer.toBytes(b.toString)
  def fromBytes(bytes:Array[Byte]) = StringSerializer.fromBytes(bytes).toBoolean
  def toString(obj:Boolean) = obj.toString
  def fromString(str:String) = str.toBoolean
}

object FloatSerializer extends Serializer[Float] {
  val bytesPerFloat = java.lang.Float.SIZE / java.lang.Byte.SIZE

  def toBytes(f:Float) = ByteBuffer.wrap(new Array[Byte](bytesPerFloat)).putFloat(f).array()
  def fromBytes(bytes:Array[Byte]) = ByteBuffer.wrap(bytes).getFloat()
  def toString(obj:Float) = obj.toString
  def fromString(str:String) = str.toFloat
}

object DoubleSerializer extends Serializer[Double] {
  val bytesPerDouble = java.lang.Double.SIZE / java.lang.Byte.SIZE

  def toBytes(d:Double) = ByteBuffer.wrap(new Array[Byte](bytesPerDouble)).putDouble(d).array()
  def fromBytes(bytes:Array[Byte]) = ByteBuffer.wrap(bytes).getDouble
  def toString(obj:Double) = obj.toString
  def fromString(str:String) = str.toDouble
}

object DateSerializer extends Serializer[Date] {
  def toBytes(date:Date) = LongSerializer.toBytes(date.getTime)
  def fromBytes(bytes:Array[Byte]) = new Date(LongSerializer.fromBytes(bytes).longValue)
  def toString(obj:Date) = obj.getTime.toString
  def fromString(str:String) = new Date(str.toLong.longValue)
}