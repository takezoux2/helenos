package com.geishatokyo.helenos.crudify

import org.slf4j.LoggerFactory
import com.geishatokyo.helenos.column.{CFDefinition, KeyspaceDefinition}
import collection.immutable.ListMap
import com.geishatokyo.helenos.command._
import com.geishatokyo.helenos.CassandraException
import com.geishatokyo.helenos.connection.{SessionPool, Session}


/**
 * Default crudify.
 * User: takeshita
 * Create: 11/11/09 12:35
 */
object Crudify extends Crudify(Session,BehaviorOnExist.Nothing,BehaviorOnNotExist.Create)

/**
 * Util class to create new column family and keyspace.
 */
class Crudify(pool : SessionPool,
              behaviorOnExist : BehaviorOnExist.Value = BehaviorOnExist.Nothing,
              behaviorOnNotExist : BehaviorOnNotExist.Value = BehaviorOnNotExist.Create) {


  val logger = LoggerFactory.getLogger(classOf[Crudify])

  /**
   * Add keyspaces
   * @param definitions
   * @return pair of (keyspace name -> AddResult
   */
  def addKeyspaces(definitions : KeyspaceDefinition*) : Map[String, AddResult.Value] = {
    pool.systemBorrow(session => {
      ListMap(definitions.map( d => {
        val r = _addKeyspace(session,d,behaviorOnExist)
        (d.name,r)
      }) :_* )
    })
  }

  private def _addKeyspace(session : Session,  definition : KeyspaceDefinition , behaviorOnExist : BehaviorOnExist.Value) : AddResult.Value = {
    val ksDef = new DescribeKeyspace(definition.name).execute(session)
    if(ksDef != null){
      behaviorOnExist match{
        case BehaviorOnExist.Nothing => return AddResult.Nothing
        case _ =>
      }
      val onCassandra = ksDef.hashCode()
      val passed = definition.ksDef.hashCode()
      logger.debug("KD hash on cassandra:%s passed:%s".format(onCassandra,passed))
      if(!ksDef.ksDef.equals(definition.ksDef)){
        behaviorOnExist match{
          case BehaviorOnExist.DropThenAdd => {
            logger.info("Drop keyspace %s and then add".format(definition.name))
            new DropKeyspace(definition.name).execute(session)
            new AddKeyspace(definition).execute(session)
            AddResult.DropAndAdd
          }
          case BehaviorOnExist.Update => {
            logger.info("Update keyspace %s".format(definition.name))
            new UpdateKeyspace(definition).execute(session)
            AddResult.Update
          }
        }
      }else{
        AddResult.Nothing
      }
    }else{
      logger.info("Add keyspace %s".format(definition.name))
      new AddKeyspace(definition).execute(session)
      AddResult.Add
    }
  }

  /**
   * Create new column families.
   * @param definitions
   * @return pare of (column family name -> AddResult
   */
  def addColumnFamilies( definitions : CFDefinition*) : Map[String, AddResult.Value] = {
    val grouped = definitions.groupBy(d => d.keyspace)
    var keyspaces = pool.systemBorrow(session => {
      new DescribeKeyspaces().execute(session)
    })
    val keyspaceNames = keyspaces.map(_.name)
    val (exists,notExists) = grouped.keys.partition( keyspaceNames.contains(_))

    var results : Map[String, AddResult.Value] = Map.empty

    logger.debug(exists + " : " + notExists)

    val targetGroups = if(notExists.size > 0 ){
      behaviorOnNotExist match{
        case BehaviorOnNotExist.ThrowError =>{
          logger.info("keyspace:%s do not exist".format(notExists))
          throw new CassandraException("Next keyspaces don't exist!" + notExists)
        }
        case BehaviorOnNotExist.Skip => {
          logger.info("keyspace:%s do not exist".format(notExists))
          results = results ++ notExists.map( d => d -> AddResult.Nothing)
          grouped.filter(g => exists.toList.contains(g._1))
        }
        case BehaviorOnNotExist.Create => {
          logger.debug("Add keyspaces:%s".format(notExists))
          addKeyspaces(notExists.map(new KeyspaceDefinition(_)).toList :_*)
          keyspaces = pool.systemBorrow(session => {
            new DescribeKeyspaces().execute(session)
          })
          grouped
        }
      }
    }else definitions.groupBy(d => d.keyspace)

    grouped.foreach( g => {
      val columns = g._2
      val keyspace = keyspaces.find(_.name == g._1).get
      pool.borrow(keyspace.name)(session => {
        for(c <- columns){
          val onCassandra = keyspace.getCF(c.name)
          if(onCassandra == null){
            new AddColumnFamily(c).execute(session)
            results = results + (c.name -> AddResult.Add)
          }else{
            if(onCassandra.hashCode != c.hashCode()){
              behaviorOnExist match{
                case BehaviorOnExist.Nothing => {
                  results = results + (c.name -> AddResult.Nothing)
                }
                case BehaviorOnExist.Update => {
                  new UpdateColumnFamily(c).execute(session)
                  results = results + (c.name -> AddResult.Update)

                }
                case BehaviorOnExist.DropThenAdd => {
                  new DropColumnFamily(c.keyspace,c.name).execute(session)
                  new AddColumnFamily(c).execute(session)
                  results = results + (c.name -> AddResult.DropAndAdd)

                }
              }
            }else{
              results = results + (c.name -> AddResult.Nothing)
            }
          }
      }
      })


    })

    results
  }

  /**
   * Drop keyspaces if exists.
   */
  def dropKeyspaces( keyspaces : String*) = {
    pool.systemBorrow(session => {
      for(ks <- keyspaces){
        new DropKeyspace(ks).execute(session)
      }
    })
  }

}

object BehaviorOnExist extends Enumeration{

  /**
   * If there is a same name keyspace or column family,
   * do nothing.(not update its)
   */
  val Nothing = Value("Nothing")
  /**
   * If there is a same name keyspace or column family and they differ,
   * update its.,
   *
  */
  val Update = Value("Update")
  /**
   * If there is a same name keyspace or column family and they differ,
   * drop then add.
   *
  */
  val DropThenAdd = Value("DropThenAdd")

}

/**
 * Behavior if keyspace is not exists when add column family.
 */
object BehaviorOnNotExist extends Enumeration{
  /**
   * Skip adding columnfamily if its keyspace does not exist.
   */
  val Skip = Value("Skip")
  /**
   * throw error if column family's keyspace does not exist.
   */
  val ThrowError = Value("ThrowError")
  /**
   * Create new keyspace by default setting if column family's keyspace does not exist.
   */
  val Create = Value("Create")
}
