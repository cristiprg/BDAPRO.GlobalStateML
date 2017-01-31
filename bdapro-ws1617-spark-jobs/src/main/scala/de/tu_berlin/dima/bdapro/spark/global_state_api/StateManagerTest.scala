package de.tu_berlin.dima.bdapro.spark.global_state_api

import org.apache.spark.mllib.linalg.{Vectors, Vector, Matrices, Matrix}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

class StateManagerTest extends FunSuite {

  var jedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(1)

  /**
    * Testes whether local (sparse) matrices are stored correctly. This test sets and gets the a small matrix and
    * compares the values.
    */
  test("Simple Set/Get State LocalMatrix") {


    val pool: JedisPool = new JedisPool(jedisPoolConfig, "localhost")
    val stateManager = new StateManager(pool)
    val matrix1: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    val matrix2: Matrix = Matrices.dense(4, 2, Array(7.0, 9.0, 11.0, 13.0, 8.0, 10.0, 12.0, 14.0))

    // Test
    assert(!localMatricesAreEqual(matrix1, matrix2))
    testLocalMatrix(stateManager, matrix1)
    testLocalMatrix(stateManager, matrix2)
    testLocalMatrix(stateManager, matrix1)
    testLocalMatrix(stateManager, matrix2)
  }

  /**
    * Testes whether arrays of local dense vectors are stored correctly. This test sets and gets the a small vector and
    * compares the values.
    */
  test("Simple Set/Get State Array of Vectors") {

    val pool: JedisPool = new JedisPool(jedisPoolConfig, "localhost")
    val stateManager = new StateManager(pool)

    val array1: Array[Vector] = Array(Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(3.0, 2.0, 1.0))
    val array2: Array[Vector] = Array(Vectors.dense(1.0, 2.0, 3.0, 4.0), Vectors.dense(4.0, 3.0, 2.0, 1.0), Vectors.dense(1.5, 1.5, 1.5, 1.5))

    // Test
    assert(!arrayOfVectorsAreEqual(array1, array2))
    testArrayOfVectors(stateManager, array1)
    testArrayOfVectors(stateManager, array2)
    testArrayOfVectors(stateManager, array1)
    testArrayOfVectors(stateManager, array2)
  }

  /**
    * Tests local vectors
    */
  test("Simple Set/Get State of State Vectors") {

    val pool: JedisPool = new JedisPool(jedisPoolConfig, "localhost")
    val stateManager = new StateManager(pool)

    val vector1: Vector = Vectors.dense(1.0, 2.0, 3.0)
    val vector2: Vector = Vectors.dense(1.0, 2.0, 3.0, 4.0)

    // Test
    assert(!vector1.equals(vector2))
    testVector(stateManager, vector1)
    testVector(stateManager, vector2)
    testVector(stateManager, vector1)
    testVector(stateManager, vector2)
  }

  private def testVector(stateManager: StateManager, vector: Vector): Unit = {
    stateManager.setStateLocalVector("key", vector)
    val retrievedVector = stateManager.getStateLocalVector("key")
    assert(vector.equals(retrievedVector))
  }

  private def testArrayOfVectors(stateManager: StateManager, array: Array[Vector]): Unit = {
    stateManager.setState(array)
    val retrievedArray = stateManager.getStateArrayOfVectors()
    assert(arrayOfVectorsAreEqual(array, retrievedArray))
  }

  private def arrayOfVectorsAreEqual(array1: Array[Vector], array2: Array[Vector]): Boolean = {
    array1.sameElements(array2)
  }

  private def testLocalMatrix(stateManager: StateManager, matrix: Matrix): Unit = {
    stateManager.setState(matrix)
    val retrievedMatrix = stateManager.getStateLocalMatrix()
    assert(localMatricesAreEqual(matrix, retrievedMatrix))
  }

  private def localMatricesAreEqual(matrix1: Matrix, matrix2: Matrix): Boolean = {
    matrix1 == matrix2 && matrix1.isTransposed == matrix2.isTransposed
  }

}
