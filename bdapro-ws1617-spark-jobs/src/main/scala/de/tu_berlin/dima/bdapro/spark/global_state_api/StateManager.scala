package de.tu_berlin.dima.bdapro.spark.global_state_api

import org.apache.spark.SparkContext
import redis.clients.jedis._

import scala.collection.JavaConversions._
import org.apache.spark.mllib.linalg._

import scala.collection.mutable.ArrayBuffer


/**
  * Stores data structures found in Spark's mllib in Redis.
  * TODO: perform independent Redis updates in pipelines
  * TODO: add generic type to class and get rid of all the getState(*) functions and use just one getState(): T
  *
  * @param REDIS_SERVER_ADDRESS
  */
class StateManager(val REDIS_SERVER_ADDRESS: String = "localhost") extends Serializable{

  //var jedis: Jedis = null
  lazy val nrRowsRedisKey: String = "nrRows"
  lazy val nrColsRedisKey: String = "nrCols"

  /**
    * Retrieves the state Local matrix from Redis. Because Local matrices are column-major, it gets a bit difficult
    * to construct one, so the way we do is: get each row, transpose it (make it column), append it to an array which
    * finally gets concatenated into a matrix.
    *
    * @return the state from Redis as a Local Matrix (Dense Matrix)
    */
  def getStateLocalMatrix(): Matrix = {

    var jedis: Jedis = null
    try {

      // Important aspect: acquire resource after getMatrixDimension() returns since the method itself acquires a
      // resource to contact Redis
      val (nrRows: Int, nrCols: Int) = getMatrixDimension()
      var arrayOfCols: Array[Matrix] = Array()

      jedis = new Jedis(REDIS_SERVER_ADDRESS)
      val pipe: Pipeline = jedis.pipelined()

      // Firstly put everything into the pipeline (and of course, sync the pipe) and then perform computations
      var responseArray: Array[Response[java.util.List[java.lang.String]]] = new Array[Response[java.util.List[java.lang.String]]](nrRows)
      0L to nrRows - 1 foreach { index =>
        val response = pipe.lrange(index.toString, 0, nrCols-1)
        responseArray(index.toInt) = response
      }
      pipe.sync()

      0L to nrRows - 1 foreach { index =>
        // Get index-th row and convert it to array of doubles (from strings)
        val row = responseArray(index.toInt).get.toList.map(x => x.toDouble).toArray
        val colMatrix = Matrices.dense(nrCols, 1, row).transpose
        arrayOfCols = arrayOfCols :+ colMatrix
      }

      Matrices.vertcat(arrayOfCols)
    }
    finally {
      if (jedis != null) jedis.close()
    }
  }

  /**
    * Same as getStateLocalMatrix, but returns an array of mllib.Vectors for convenience.
    * It's a wrapper over getStateLocalMatrix so no connection from the pool is required.
    */
  def getStateArrayOfVectors(): Array[Vector] = {
    val matrix: Matrix = getStateLocalMatrix()
    var arrayBuf = ArrayBuffer[Vector]()

    val it = matrix.rowIter
    while(it.hasNext) {
      arrayBuf += it.next
    }

    arrayBuf.toArray
  }


  /**
    * Retrieves the dimension of the matrix from Redis.
    *
    * @return a tuple containing the dimensions (nrRows, nrCols)
    */
  private def getMatrixDimension(): (Int, Int) = {
    var jedis: Jedis = null
    var nrRows: Response[String] = null
    var nrCols: Response[String] = null
    try {
      jedis = new Jedis(REDIS_SERVER_ADDRESS)
      val pipe: Pipeline = jedis.pipelined()
      // TODO possible deadlock here
      nrRows = pipe.get(nrRowsRedisKey)
      nrCols = pipe.get(nrColsRedisKey)
      pipe.sync()
    }
    finally {
      if (jedis != null) jedis.close()
    }

    // TODO return error? Throw exception?
    if (nrRows == null || nrCols == null)
      (0, 0)

    (nrRows.get.toInt, nrCols.get.toInt)
  }

  /**
    * Saves the state matrix in Redis.
    * TODO: change parameter type to generic
    *
    * @param matrix instance of Local Matrix to be stored in Redis
    */
  def setState(matrix: Matrix): Unit = {
    var jedis: Jedis = null
    try {
      jedis = new Jedis(REDIS_SERVER_ADDRESS)
      setStateMatrix(matrix, jedis)
    }
    finally {
      if (jedis != null) jedis.close()
    }
  }

  private def setStateMatrix(matrix: Matrix, jedis: Jedis): Unit = {
    val it = matrix.rowIter
    var i: Int = 0

    val pipeline = jedis.pipelined()
    setMatrixDimensions(matrix.numRows, matrix.numCols, pipeline)
    while(it.hasNext) {
      setLocalVector(i, it.next, pipeline)
      i += 1
    }
    pipeline.sync()
  }

  def setState(array: Array[Vector]): Unit = {
    if (array.length == 0)
      return

    var jedis: Jedis = null
    try {
      jedis = new Jedis(REDIS_SERVER_ADDRESS)
      setStateMatrix(array, jedis)
    }
    finally {
      if (jedis != null) jedis.close()
    }
  }

  private def setStateMatrix(array: Array[Vector], jedis: Jedis): Unit = {
    val pipeline = jedis.pipelined()
    setMatrixDimensions(array.length, array(0).size, pipeline)
    var i = 0
    array.foreach(x => {
      setLocalVector(i, x, pipeline)
      i += 1
    })
    pipeline.sync()

  }



  /**
    * Saves the dimension of the matrix to Redis.
    *
    * @param nrRows number of rows
    * @param nrCols number of columns
    */
  private def setMatrixDimensions(nrRows: Int, nrCols: Int, pipeline: Pipeline): Unit = {
    pipeline.set(nrRowsRedisKey, nrRows.toString)
    pipeline.set(nrColsRedisKey, nrCols.toString)
  }

  /**
    * Stores a Local vector considered the index-th row of a Local matrix in Redis.
    * This vector is not an RDD and thus the Jedis library is used instead of Spark-Redis.
    *
    * @param index the index of the row/vector in the matrix to be stored. This is used as key in Redis
    * @param vector the index-th row/vector in the matrix. This is used as value in Redis
    */
  private def setLocalVector(index: Int, vector: Vector, pipeline: Pipeline): Unit = {

    // Delete old key and create a new one.
    // If you're confused, check https://github.com/xetorthio/jedis/issues/566
    // There is an issue with calling Pipeline.del() from Scala and this is a workaround.
    val keyArr = Array[String](index.toString)
    pipeline.del(keyArr:_*)
    val key = keyArr(0)

    // TODO: conversion toDense might slow down too much
    vector.toDense.values.foreach(x => pipeline.rpush(key, x.toString))
  }


  /**
    * Saves the state as a Vector in Redis, identifiable by a specific key. This can be used to
    * store additional useful information besides the matrix.
    * TODO: use the private setLocalVector
    *
    * @param key the Key, the name of the vector
    * @param vector the vector to be stored in Redis
    */
  def setStateLocalVector(key: String, vector: Vector): Unit = {
//    var jedisPoolConfig = new JedisPoolConfig()
//    jedisPoolConfig.setMaxTotal(250)
//    val pool: JedisPool = new JedisPool(jedisPoolConfig, REDIS_SERVER_ADDRESS)
    var jedis: Jedis = null
    try {
      jedis = new Jedis(REDIS_SERVER_ADDRESS)
      val pipeline = jedis.pipelined()

      // Delete old key and create a new one
      // If you're confused, check https://github.com/xetorthio/jedis/issues/566
      // There is an issue with calling Pipeline.del() from Scala and this is a workaround.
      val keyArr = Array[String](key)
      pipeline.del(keyArr:_*)

      // TODO: conversion toDense might slow down too much
      vector.toDense.values.foreach(x => pipeline.rpush(key, x.toString))
      pipeline.sync()
    }
    finally {
      if (jedis != null && jedis.isConnected()) jedis.close()
    }
  }


  /**
    * Retrieves the state Vector identified by key.
    * @param key the key of the Vector you want
    * @return the Vector you want
    */
  def getStateLocalVector(key: String): DenseVector = {
//    var jedisPoolConfig = new JedisPoolConfig()
//    jedisPoolConfig.setMaxTotal(250)
//    val pool: JedisPool = new JedisPool(jedisPoolConfig, REDIS_SERVER_ADDRESS)

    var jedis: Jedis = null
    try {
      jedis = new Jedis(REDIS_SERVER_ADDRESS)
      val pipeline = jedis.pipelined()

      // Adopt the optimistic approach. First instruction in the pipeline is an "exist" instruction. However, don't
      // wait for the answer and try getting the key from Redis. In the end, if the key doesn't exist, return null.

      // Here is the same issue with .del(). Check https://github.com/xetorthio/jedis/issues/566
      val keyExists = pipeline.exists(Array[String](key):_*)

      // Put everything in the pipeline, sync, then check whether the key existed in the first place, and finally
      // construct the row.
      val responseArray = pipeline.lrange(key, 0, -1)

      pipeline.sync()
      if (keyExists.get.toInt < 1) {
        null
      }
      else {
        val row = responseArray.get.toList.map(x => x.toDouble).toArray
        Vectors.dense(row).asInstanceOf[DenseVector]
      }
    }
    finally {
      if (jedis != null && jedis.isConnected()) jedis.close()
    }
  }
}
