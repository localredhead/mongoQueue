package com.mongoQueue

import java.util.Date
import com.mongodb.casbah.Imports._
import scala.xml._
import scala._

class monQueue(connection:MongoConnection, config:Map[String, Any]) {

  def printAll() = queue.find().foreach(x => println(x))

  def queue() = connection(config.get("database").toString)(config.get("collection").toString)

  def getDB() = queue.getDB()

  def insert(record:Map[String, Any]) : AnyRef =
  {
    queue.insert(record)
    return queue.find(record)
  }

  def lockNext(lockedBy:Any) : Option[DBObject] =
  {
    return queue.findAndModify(
      MongoDBObject("attempts"  -> MongoDBObject("$lt" -> config("attempts"))) += ("locked_by" -> null),
      null,
      null,
      false,
      $set("locked_by" -> lockedBy, "locked_at" -> new Date().getTime()) ++ $inc("attempts" -> 1),
      true,
      false)
  }

  def release(queueItem:Option[DBObject], lockedBy:AnyRef) : Option[DBObject] =
  {
    return queue.findAndModify(
      MongoDBObject("_id" -> queueItem.get("_id")) += ("locked_by" -> lockedBy),
      null,
      null,
      false,
      $set("locked_by" -> null, "locked_at" -> null),
      true,
      false)
  }

  def complete(queueItem:Option[DBObject], lockedBy:AnyRef) : Option[DBObject] =
  {
    return queue.findAndRemove(MongoDBObject("_id" -> queueItem.get("_id")) += ("locked_by" -> lockedBy))
  }

  def cleanUp()
  {
    val time = new Date().getTime().asInstanceOf[Long] - config("timeout").asInstanceOf[Long]
    println(new Date().getTime().asInstanceOf[Int].toString)
    println(config("timeout").asInstanceOf[Int].toString)
    println(time.toString)
    val cursor = queue().find(
      MongoDBObject("locked_at"  -> ("$lt" -> time)))

    cursor.foreach( x => this.release(queue.findOneByID(x.get("_id")), x.get("locked_by")))
  }

  def flush()
  {
    queue.drop();
  }

}

object monQueue{
  val DEFAULT_INSERT = Map(
    "priority"   -> null,
    "attempts"   -> 0,
    "locked_by"  -> null,
    "locked_at"  -> null,
    "last_error" -> null)

  val DEFAULT_CONFIG = Map(
    "database"   -> "mongo_queue",
    "collection" -> "mongo_queue",
    "timeout"    -> 300,
    "attempts"   -> 3)
}

