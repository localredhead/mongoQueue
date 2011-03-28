package com.mongoQueue

import com.mongodb.casbah.Imports._
import scala.xml._
import scala._

object monCon {

  def main(args: Array[String]) {

  /*Test getting args from cmd line*/
    //args.foreach(x => println(x))

    val test:monQueue = new monQueue(MongoConnection(), monQueue.DEFAULT_CONFIG)

    test.insert(monQueue.DEFAULT_INSERT)

    test.lockNext("Levi Strope - super Admin")
    test.printAll()

    //test.flush()



  }

}