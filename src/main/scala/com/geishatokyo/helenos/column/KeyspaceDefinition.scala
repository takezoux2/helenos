package com.geishatokyo.helenos.column

import com.geishatokyo.helenos.StringUtil
import org.apache.cassandra.thrift.{IndexType, ColumnDef, CfDef, KsDef}
import scala.collection.JavaConverters._
import java.util.{ArrayList, Arrays, HashMap}


/**
 * 
 * User: takeshita
 * Create: 11/11/09 11:59
 */

class KeyspaceDefinition(val ksDef : KsDef) {

  def name = ksDef.getName

  def this(name : String) = {
    this( new KsDef())
    ksDef.setName(name)
    ksDef.setStrategy_class(StrategyClass.NetworkTopology)
    ksDef.setCf_defs(new java.util.ArrayList[CfDef]())
    ksDef.setStrategy_options(new java.util.HashMap[String, String]())
    ksDef.setDurable_writes(true)
    ksDef.setStrategy_options(Map("datacenter1" -> "1").asJava)
  }


  private def _indexOf(columnFamilyName : String) = {
    var find = false
    val cfs = ksDef.getCf_defs
    var index = -1
    for(i <- 0 until cfs.size() if !find){
      if(cfs.get(i).getName == columnFamilyName){
        index = i
        find = true
      }
    }
    index

  }

  private def removeCF(columnFamilyName : String) = {
    val index = _indexOf(columnFamilyName)
    if(index >= 0){
      val l =    ksDef.getCf_defs()
      l.remove(index)
      ksDef.setCf_defs(l)
    }
  }

  def +=(cf : CfDef): KeyspaceDefinition = {
    removeCF(cf.getName)
    ksDef.getCf_defs.add(cf)

    this
  }
  def +=(cf : CFDefinition) : KeyspaceDefinition = {
    this.+=(cf.cfDef)

    this
  }

  def ++=( cfs : List[CFDefinition]) : KeyspaceDefinition = {
    cfs.foreach(this.+=(_))

    this
  }

  def addStandard( name : String , modify : CFDefinition => CFDefinition) = {
    val cf = modify( CFDefinition.standardCF(this.name,name))
    removeCF(name)
    ksDef.getCf_defs.add(cf.cfDef)
    this
  }

  def addSuper(name : String,  modify : CFDefinition => CFDefinition) = {
    val cf = modify( CFDefinition.superCF(this.name,name))
    removeCF(name)
    ksDef.getCf_defs.add(cf.cfDef)
    this
  }

  def -=(columnFamilyName : String) : Unit = {
    removeCF(columnFamilyName)
  }

  def indexOf(columnFamilyName : String) : Int = {
    _indexOf(columnFamilyName)
  }

  def getCF(columnFamilyName : String) : CFDefinition = {
    val index = _indexOf(columnFamilyName)
    if(index >= 0){
      new CFDefinition(ksDef.getCf_defs().get(index))
    }else{
      null
    }
  }

  override def hashCode() = {
    ksDef.hashCode()
  }

  override def equals(obj: Any) = {
    obj match{
      case kd : KeyspaceDefinition => {
        ksDef.equals(kd.ksDef)
      }
      case _ => {
        ksDef.equals(obj)
      }
    }
  }
}

object CFDefinition{
  def standardCF( keyspace : String, name : String) = {
    val cf = new CFDefinition(keyspace,name)

    cf
  }

  def superCF( keyspace : String, name : String) = {
    val cf = new CFDefinition(keyspace,name)
    cf.setSuperCF(true)
    cf
  }

  /**
   * returns typical text type standard column family.
   */
  def typicalTextStandardCF( keyspace : String, name : String) = {
    new CFDefinition(keyspace,name).withComparator(ComparatorType.UTF8Type).withValidator(ComparatorType.UTF8Type)
  }

  /**
   * returns typical text type super column family.
   */
  def typicalTextSuperCF( keyspace : String, name : String) = {
    new CFDefinition(keyspace,name).setSuperCF(true).withComparator(ComparatorType.UTF8Type).withValidator(ComparatorType.UTF8Type)
  }

  def typicalStandardCounterCF(keyspace : String, name : String) = {
    typicalTextStandardCF(keyspace,name).withValidator(ComparatorType.CounterColumnType)
  }
  def typicalSuperCounterCF(keyspace : String, name : String) = {
    typicalTextSuperCF(keyspace,name).withValidator(ComparatorType.CounterColumnType)
  }

}

class CFDefinition(val cfDef : CfDef){

  def keyspace = cfDef.getKeyspace
  def name = cfDef.getName

  def this(keyspace : String, name : String) = {
    this( new CfDef())
    cfDef.clear()
    cfDef.setName(name)
    cfDef.setKeyspace(keyspace)

  }


  val superCF_? = cfDef.getColumn_type == ColumnType.Super
  val standardCF_? = cfDef.getColumn_type == ColumnType.Standard

  def setSuperCF( super_? : Boolean) = {
    if(super_?){
      cfDef.setColumn_type(ColumnType.Super)
      if(!cfDef.isSetSubcomparator_type()){
        cfDef.setSubcomparator_type(ComparatorType.BytesType)
      }
    }else{
      cfDef.setColumn_type(ColumnType.Standard)
    }
    this
  }

  def withValidator(validator : String) = {
    cfDef.setDefault_validation_class(validator)
    this
  }

  def withComparator(comparator : String) = {
    cfDef.setComparator_type(comparator)
    this
  }

  def withSubComparator(comparator : String) = {
    cfDef.setSubcomparator_type(comparator)
    this
  }

  def withKeyValidator(validatorClass : String) = {
    cfDef.setKey_validation_class(validatorClass)
    this
  }

  def @@(paramName : String, v : Any) : CFDefinition = withParam(paramName,v)
  def @@(param : (String, Any)*) : CFDefinition = {
    for(p <- param){
      withParam(p._1,p._2)
    }
    this
  }

  def withParam( paramName : String, v : Any) = {
    val m = classOf[CfDef].getMethod("set" + StringUtil.toInitialUpperCase(paramName))
    m.invoke(cfDef,v.asInstanceOf[AnyRef])
    this
  }

  private def indexOfMetadata(columnName : Array[Byte]) : Int = {
    val indexes = cfDef.getColumn_metadata()
    if(indexes == null) return -1
    for(i <- 0 until indexes.size()){
      val index = indexes.get(i)
      if(Arrays.equals(columnName,index.getName)){
        return i
      }
    }
    -1
  }

  private def indexOfIndex(indexName : String) : Int = {
    val indexes = cfDef.getColumn_metadata()
    if(indexes == null) return -1
    for(i <- 0 until indexes.size()){
      val index = indexes.get(i)
      if(index.getIndex_name == indexName){
        return i
      }
    }
    -1
  }

  private def removeMetadata(columnName : Array[Byte]) = {
    val index = indexOfMetadata(columnName)
    if(index >= 0){
      val l = cfDef.getColumn_metadata()
      l.remove(index)
      cfDef.setColumn_metadata(l)
    }
  }
  def :+=( columnDef : ColumnDefinition ) : CFDefinition = {
    this.:+=(columnDef.columnDef)
  }

  def :+=( columnDef : ColumnDef ) : CFDefinition = {
    removeMetadata(columnDef.getName)
    val l = if(cfDef.getColumn_metadata() == null) new ArrayList[ColumnDef]() else cfDef.getColumn_metadata
    l.add(columnDef)
    cfDef.setColumn_metadata(l)

    this
  }

  override def hashCode() = {
    cfDef.hashCode()
  }

  override def equals(obj: Any) = {
    cfDef.equals(obj)
  }
}

object ColumnDefinition{
  def index(columnName : String,validator : String = ComparatorType.BytesType) = {
    new ColumnDefinition(columnName.getBytes("UTF8"),validator).withIndexName("INDEX_" + columnName)
  }

  def counter(columnName : String) = {
    new ColumnDefinition(columnName.getBytes("UTF8"),ComparatorType.CounterColumnType)
  }
}

class ColumnDefinition(val columnDef : ColumnDef){

  def this(columnName : Array[Byte], validationClass : String) = {
    this(new ColumnDef())
    columnDef.clear()
    columnDef.setName(columnName)
    columnDef.setValidation_class(validationClass)
  }

  def this(columnName : Array[Byte]) = {
    this(columnName,ComparatorType.BytesType)
  }


  def withValidationClass( validationClass : String) = {
    columnDef.setValidation_class(validationClass)
    this
  }

  def withIndexType( indexType : IndexType) = {
    columnDef.setIndex_type(indexType)
    this
  }

  def withIndexName( indexName : String) = {
    columnDef.setIndex_name(indexName)
    if(!columnDef.isSetIndex_type){
      columnDef.setIndex_type(IndexType.KEYS)
    }
    this
  }

  def withIndexOptions( options : Map[String, String]) = {
    columnDef.setIndex_options(options.asJava)

    this
  }

  override def hashCode() = {
    columnDef.hashCode()
  }

  override def equals(obj: Any) = {
    columnDef.equals(obj)
  }
}

object StrategyClass{

  val Simple = "org.apache.cassandra.locator.SimpleStrategy"
  val NetworkTopology = "org.apache.cassandra.locator.NetworkTopologyStrategy"
  val OldNetworkTopology = "org.apache.cassandra.locator.OldNetworkTopologyStrategy"

}

object ColumnType{
  val Standard = "Standard"
  val Super = "Super"
}

object ComparatorType{

  val SuperColumns = "SuperColumns"
  val BytesType = "BytesType"
  val AsciiType = "AsciiType"
  val UTF8Type = "UTF8Type"
  val LongType = "LongType"
  val LexicalUUIDType = "LexicalUUIDType"
  val TimeUUIDType = "TimeUUIDType"

  val CounterColumnType = "CounterColumnType"
  val Int32Type = "Int32Type"
  val IntegerType = "IntegerType"

}

