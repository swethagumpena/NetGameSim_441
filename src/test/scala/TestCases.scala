import com.google.common.graph.MutableValueGraph
import com.google.common.graph.ValueGraphBuilder
import com.lsc.utils
import com.lsc.utils.WriteNodePairsToFile
import com.lsc.utils.WriteEdgePairsToFile
import com.lsc.LikelihoodComputation.findBestScore
import com.lsc.utils.GoodnessEstimation
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.io.File
import java.text.SimpleDateFormat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger
import org.mockito.Mockito._

import scala.io.Source
import scala.runtime.stdLibPatches.Predef.assert
import utils.SimilarityScoreCalculation.jaccardSimilarity
import NetGraphAlgebraDefs.Action
import NetGraphAlgebraDefs.NetGraph
import NetGraphAlgebraDefs.NetStateMachine
import NetGraphAlgebraDefs.NodeObject
import Utilz.CreateLogger
import com.lsc.NodesSimScore.NodesSimScoreMap

class TestCases extends AnyFunSuite with Matchers {
  val logger: Logger = CreateLogger(this.getClass)

  // MainClass.scala
  test("Check if config loads") {
    // test if the config file loads or not
    val config: Config = ConfigFactory.load()
    val pathConfig = config.getConfig("NGSimulator").getConfig("OutputPath")
    assert(!pathConfig.isEmpty)
  }

  // MainClass.scala
  test("Check if outputpath exists for different jobs") {
    // test if the config for OutputPath for different jobs is present in the application.conf file
    val config: Config = ConfigFactory.load()
    val pathConfig = config.getConfig("NGSimulator").getConfig("OutputPath")
    val outputpath_a = pathConfig.getString("nodePairs")
    val outputpath_b = pathConfig.getString("edgePairs")
    val outputpath_c = pathConfig.getString("nodesSimScore")
    val outputpath_d = pathConfig.getString("edgesSimScore")
    val outputpath_e = pathConfig.getString("likelihoodComputation")
    assert(
      outputpath_a.nonEmpty && outputpath_b.nonEmpty && outputpath_c.nonEmpty && outputpath_d.nonEmpty && outputpath_e.nonEmpty)
  }

  val node1: NodeObject = NodeObject(id = 1, children = 2, props = 3, currentDepth = 4, propValueRange = 5, maxDepth = 6, maxBranchingFactor = 7, maxProperties = 8, storedValue = 9.0)
  val node2: NodeObject = NodeObject(id = 10, children = 11, props = 12, currentDepth = 13, propValueRange = 14, maxDepth = 15, maxBranchingFactor = 16, maxProperties = 17, storedValue = 18.0)
  val node3: NodeObject = NodeObject(id = 20, children = 21, props = 22, currentDepth = 23, propValueRange = 24, maxDepth = 25, maxBranchingFactor = 26, maxProperties = 27, storedValue = 28.0)
  val node4: NodeObject = NodeObject(id = 30, children = 31, props = 32, currentDepth = 33, propValueRange = 34, maxDepth = 35, maxBranchingFactor = 36, maxProperties = 37, storedValue = 38.0)
  val edge12: Action = Action(actionType = 1, node1, node2, fromId = 1, toId = 2, resultingValue = Some(12), cost = 0.12)
  val edge23: Action = Action(actionType = 2, node2, node3, fromId = 2, toId = 3, resultingValue = Some(23), cost = 0.23)
  val edge34: Action = Action(actionType = 3, node3, node4, fromId = 3, toId = 4, resultingValue = Some(34), cost = 0.34)

  def createTestGraph(): NetGraph = {
    val graph1: MutableValueGraph[NodeObject, Action] = ValueGraphBuilder.directed().build()

    if !graph1.addNode(node1) then logger.error(s"Node $node1 already exists")
    if !graph1.addNode(node2) then logger.error(s"Node $node2 already exists")
    if !graph1.addNode(node3) then logger.error(s"Node $node3 already exists")
    graph1.putEdgeValue(node1, node2, edge12)
    graph1.putEdgeValue(node2, node3, edge23)
    graph1.putEdgeValue(node3, node4, edge34)
    NetGraph(graph1, node1)
  }

  // WriteNodePairsToFile.scala
  test("Create node pairs and write to file") {
    val ogGraph = createTestGraph()
    val pbGraph = createTestGraph()

    val filePath = "src/test/scala/test_node_pairs.txt"
    WriteNodePairsToFile.createNodePairsAndWrite(ogGraph.sm, pbGraph.sm, filePath)

    // Read the file and validate its contents
    val source = Source.fromFile(filePath)
    val fileContents = source.getLines().toList
    source.close()

    assert(fileContents.size == 4)
    assert(
      fileContents.head == "NodeObject(1,2,3,4,5,6,7,8,9.0), NodeObject(10,11,12,13,14,15,16,17,18.0) x NodeObject(1,2,3,4,5,6,7,8,9.0), NodeObject(10,11,12,13,14,15,16,17,18.0)")
  }

  // WriteEdgePairsToFile.scala
  test("Create edge pairs and write to file") {
    val ogGraph = createTestGraph()
    val pbGraph = createTestGraph()

    val filePath = "src/test/scala/test_edge_pairs.txt"
    WriteEdgePairsToFile.createEdgePairsAndWrite(ogGraph.sm, pbGraph.sm, filePath)

    // Read the file and validate its contents
    val source = Source.fromFile(filePath)
    val fileContents = source.getLines().toList
    source.close()

    assert(fileContents.size == 4)
    assert(
      fileContents.head == "(NodeObject(1,2,3,4,5,6,7,8,9.0)-NodeObject(10,11,12,13,14,15,16,17,18.0)-1-0.12) | (NodeObject(10,11,12,13,14,15,16,17,18.0)-NodeObject(20,21,22,23,24,25,26,27,28.0)-2-0.23) x (NodeObject(1,2,3,4,5,6,7,8,9.0)-NodeObject(10,11,12,13,14,15,16,17,18.0)-1-0.12) | (NodeObject(10,11,12,13,14,15,16,17,18.0)-NodeObject(20,21,22,23,24,25,26,27,28.0)-2-0.23)")
  }

  // SimilarityScoreCalculation.scala
  test("Check if Jaccard Similarity returns the right result for unequal values negative") {
    val set1 = Set(1, 2, 3, 4, 5)
    val set2 = Set(1, 2, 3, 6, 7)

    val similarity = jaccardSimilarity(set1, set2)
    // intersection is 3 (1,2,3). Union is 7 (1,2,3,4,5,6,7). 3 / 7 ≈ 0.4286
    similarity shouldEqual 0.4286 +- 0.0001
  }

  // SimilarityScoreCalculation.scala
  test("Check if Jaccard Similarity returns the right result for equal values") {
    val set1 = Set(1, 2, 3, 4, 5)
    val set2 = Set(1, 2, 3, 4, 5)

    val similarity = jaccardSimilarity(set1, set2)
    similarity shouldEqual 1.0
  }

  // SimilarityScoreCalculation.scala
  test("Check if Jaccard Similarity returns the right result for all different values") {
    val set1 = Set(1, 2, 3, 4, 5)
    val set2 = Set(6, 7, 8, 9, 10)

    val similarity = jaccardSimilarity(set1, set2)
    similarity shouldEqual 0.0
  }

  // LikelihoodComputation.scala
  test("findBestScore should correctly determine the best score") {
    // to find the best score that is subsequently used to decide if a node/edge has been added/modified/removed/unchanged
    val key1 = "O_1"
    val value1 = "A=0.1, B=0.2, C=0.3"
    // if the key is prepended with O_ the best score is the max value
    findBestScore(key1, value1) shouldEqual("1", 0.3)

    val key2 = "P_2"
    val value2 = "X=0.8, Y=0.9, Z=0.7"
    // if the key is prepended with P_ the best score is 0.0 if there are no values between 0.1 and 0.9
    findBestScore(key2, value2) shouldEqual("2", 0.0)
  }

  // GoodnessEstimation.scala
  test("calculateMetrics should correctly calculate metrics") {
    val (atl, dtl, ctl, wtl) = (2, 1, 3, 4)
    val (acc, btlr, vpr) = GoodnessEstimation.calculateMetrics(atl, dtl, ctl, wtl)
    assert(acc == 0.2)
    assert(btlr == 0.4)
    assert(vpr == 0.3)
  }

  // GoodnessEstimation.scala
  test("getBucketName should extract bucket name from S3 path") {
    val s3Path = "s3://my-bucket/my-file.txt"
    val bucketName = GoodnessEstimation.getBucketName(s3Path)
    assert(bucketName == "my-bucket")
  }

  // GoodnessEstimation.scala
  test("getKey should extract key from S3 path") {
    val s3Path = "s3://my-bucket/my-file.txt"
    val key = GoodnessEstimation.getKey(s3Path)
    assert(key == "my-file.txt")
  }
}