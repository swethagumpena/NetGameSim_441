package NetGraphAlgebraDefs
import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NetGraphAlgebraDefs.{NetGraph, NetModel, NetModelAlgebra}
import NetModelAnalyzer.RandomWalker
import Randomizer.SupplierOfRandomness
import Utilz.ConfigReader.getConfigEntry
import Utilz.CreateLogger
import org.apache.commons.io.FileUtils
import org.slf4j.Logger
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
object ReadGraph extends App{
  val logger: Logger = CreateLogger(this.getClass)
  logger.info(outputDirectory)

  val netGraph = NetGraph.load("/outputs/NetGameSimNetGraph_11-09-23-15-05-01.ngs", outputDirectory)
  if netGraph.isEmpty then logger.info("Empty")

  val head = netGraph.head.sm
  logger.info(head.toString)

  var loopsInGraph = 0

  logger.info(" ")
  val immediateChildren: Set[NodeObject] = head.successors(netGraph.head.sm.nodes().asScala.find(_.id == 0).get).asScala.toSet
  logger.info("immediateChildren" + immediateChildren)
  
  @tailrec
  def dfs(nodes: List[NodeObject], visited: Set[NodeObject]): Set[NodeObject] =
    if visited.size.toFloat / (head.nodes().size - 1).toFloat >= 1.0 then visited
    else
      nodes match
        case Nil => visited
        case hd :: tl =>
          if visited.contains(hd) then
            loopsInGraph += 1
            dfs(tl, visited)
          else
            logger.info(hd.id + "")
            dfs(head.successors(hd).asScala.toList.filterNot(n => visited.contains(n)) ::: tl, visited + hd) // ++ dfs(tl, visited + hd)
  end dfs

  dfs(head.nodes().asScala.toList, Set())
}