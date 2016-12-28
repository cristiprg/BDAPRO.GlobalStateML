package de.tu_berlin.dima.bdapro.spark.global_state_api

import org.apache.spark.SparkContext
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

import org.apache.spark.mllib.linalg.{Matrices, Vector, Matrix}


/**
  * Stores data structures found in Spark's mllib in Redis.
  * TODO: add generic type to class and get rid of all the getState(*) functions and use just one getState(): T
  *
  * @param sc the SparkContext instance used in your program
  */
class StateManager(val sc: SparkContext) {

  val jedis = new Jedis("localhost") // TODO: connect to whatever exists in the SparkContext
  val nrRowsRedisKey: String = "nrRows"
  val nrColsRedisKey: String = "nrCols"

  /**
    * Retrieves the state Local matrix from Redis. Because Local matrices are column-major, it gets a bit difficult
    * to construct one, so the way we do is: get each row, transpose it (make it column), append it to an array which
    * finally gets concatenated into a matrix.
    *
    * @return the state from Redis as a Local Matrix (Dense Matrix)
    */
  def getStateLocalMatrix(): Matrix = {
    val (nrRows: Int, nrCols: Int) = getMatrixDimension()
    var arrayOfCols: Array[Matrix] = Array()

    0L to nrRows-1 foreach{ index =>
      // Get index-th row and convert it to array of doubles (from strings)
      val row = jedis.lrange(index.toString, 0, -1).toList.map(x => x.toDouble).toArray
      val colMatrix = Matrices.dense(nrCols, 1, row).transpose
      arrayOfCols = arrayOfCols :+ colMatrix
    }

    Matrices.vertcat(arrayOfCols)
  }


  /**
    * Retrieves the dimension of the matrix from Redis.
    *
    * @return a tuple containing the dimensions (nrRows, nrCols)
    */
  private def getMatrixDimension(): (Int, Int) = {
    val nrRows = jedis.get(nrRowsRedisKey).toInt
    val nrCols = jedis.get(nrColsRedisKey).toInt

    (nrRows, nrCols)
  }

  /**
    * Saves the state matrix in Redis.
    * TODO: change parameter type to generic
    *
    * @param matrix instance of Local Matrix to be stored in Redis
    */
  def setState(matrix: Matrix): Unit = {
    setStateMatrix(matrix)
  }

  private def setStateMatrix(matrix: Matrix): Unit = {
    val it = matrix.rowIter
    var i: Int = 0

    setMatrixDimensions(matrix.numRows, matrix.numCols)
    while(it.hasNext) {
      setLocalVector(i, it.next)
      i += 1
    }
  }

  /**
    * Saves the dimension of the matrix to Redis.
    *
    * @param nrRows number of rows
    * @param nrCols number of columns
    */
  private def setMatrixDimensions(nrRows: Int, nrCols: Int): Unit = {
    jedis.set(nrRowsRedisKey, nrRows.toString)
    jedis.set(nrColsRedisKey, nrCols.toString)
  }

  /**
    * Stores a Local vector considered the index-th row of a Local matrix in Redis.
    * This vector is not an RDD and thus the Jedis library is used instead of Spark-Redis.
    *
    * @param index the index of the row/vector in the matrix to be stored. This is used as key in Redis
    * @param vector the index-th row/vector in the matrix. This is used as value in Redis
    */
  private def setLocalVector(index: Int, vector: Vector): Unit = {

    val key = index.toString
    val lengthInRedis = jedis.llen(key)

    if (!jedis.exists(key)) {
      // TODO: conversion toDense might slow down too much
      vector.toDense.values.foreach(x => jedis.rpush(key, x.toString))
    }
    else
      vector.toDense.foreachActive((index, value) => {
        jedis.lset(key, index.toLong, value.toString)
        Unit
      })
  }
}
