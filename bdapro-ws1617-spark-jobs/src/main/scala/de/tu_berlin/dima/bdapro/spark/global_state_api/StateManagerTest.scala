package de.tu_berlin.dima.bdapro.spark.global_state_api

import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class StateManagerTest extends FunSuite {

  /**
    * Testes whether local (sparse) matrices are stored correctly. This test sets and gets the a small matrix and
    * compares the values.
    */
  test("Simple Set/Get State LocalMatrix") {

    // Init
    val sc = new SparkContext(new SparkConf()
      .setAppName("bdapro-globalstate")
      .setMaster("local"))
    val stateManager = new StateManager(sc)
    val matrix1: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    val matrix2: Matrix = Matrices.dense(4, 2, Array(7.0, 9.0, 11.0, 13.0, 8.0, 10.0, 12.0, 14.0))

    // Test
    assert(!localMatricesAreEqual(matrix1, matrix2))
    testLocalMatrix(stateManager, matrix1)
    testLocalMatrix(stateManager, matrix2)
    testLocalMatrix(stateManager, matrix1)
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
