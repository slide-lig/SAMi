package fr.liglab.sami

import java.io.Serializable
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Calendar
import java.util.Collections

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.cli.PosixParser

import fr.liglab.sami.code.DAG
import fr.liglab.sami.code.DFSCode
import fr.liglab.sami.code.DagProvider
import fr.liglab.sami.code.Edge
import fr.liglab.sami.code.FrequentGraph
import fr.liglab.sami.data.ConfigLoader
import fr.liglab.sami.data.GraphLoader
import fr.liglab.sami.utilities.StopWatch
import org.apache.commons.cli.DefaultParser
import fr.liglab.sami.data.PreGroupedGraph
import fr.liglab.sami.data.GraphInfo

// as soon as we have at least n>1 edges, all graphs of size n+1 can be obtained by merging 2 graphs of size n having n-1 edges in common
// => using lexicographic order on edges we could have a type of first parent for instances
// pb is this first parent is not linked to the structure of the graph, so we need to make DFS canonical
// standard DFS based approach has a structural first parent

class Sami(
    private val broadcastedGraph: collection.Map[Edge, DAG],
    private val minSupport: Int,
    private val numPartitions: Int,
    private val nodeMap: Map[String, Int],
    private val maxPatternLength: Int,
    private val maxThreads: Int,
    private val dispEvery: Int) extends Serializable {

  def run: List[FrequentGraph] = {
    val frequentGraphs = Collections.synchronizedList(new ArrayList[FrequentGraph]())
    var cache: GraphInfo = new PreGroupedGraph(broadcastedGraph)
    DagProvider.graphData = cache
    val workPool = new Workpool(minSupport, maxPatternLength, cache, maxThreads, nodeMap, dispEvery)
    val seeds = cache.getGraph.map(_._1).filter(s => (new DFSCode(Vector(s), 2).isCanonical)).toList.sorted
    println("nb seeds " + seeds.size)
    workPool.init(seeds, numPartitions)
    println("workpool initialized")
    for (i <- 0 until maxThreads) {
      new Thread("StealSpan worker " + i) {
        override def run(): Unit = {
          while (true) {
            val task = workPool.getLocalWork()
            if (task == null) return
            task.execute()
          }
        }
      }.start()
    }
    val cal: Calendar = Calendar.getInstance();
    val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss");
    // println(">>>>>>>>>>>>>>>>>>>> pool.shutdown() runningTasks: "+runningTasks.get+" @ "+sdf.format(cal.getTime()) +"  <<<<<<<<<<<<<<<<<<<<<<<<<<<")
    // println(">>>>>>>>>>>>>>>>>>>> awaitTermination complete  <<<<<<<<<<<<<<<<<<<<<<<<<<<")
    workPool.frequentPatterns.synchronized {
      if (!workPool.terminateThreads) {
        workPool.frequentPatterns.wait()
      }
    }
    return workPool.frequentPatterns.toList
  }
}

object SamiRunner {

  def smarterRoundRobin(edgeIndex: Int, partitionIndex: Int, nbPartitions: Int): Boolean = {
    if ((edgeIndex / nbPartitions) % 2 == 0) {
      edgeIndex % nbPartitions == partitionIndex
    } else {
      edgeIndex % nbPartitions == nbPartitions - (partitionIndex + 1)
    }
  }
  def printMan(options: Options) {
    val syntax = "scala fr.liglab.sami.SamiRunner [OPTIONS]";
    val header = "\nIf OUTPUT_PATH is missing, patterns are printed to standard output.\nOptions are :";
    val footer = "Version 1.0 - Copyright 2017 Vincent Leroy, Nicholas Caldwell, Université Grenoble Alpes, Laboratoire d'Informatique de Grenoble, CNRS"

    val formatter: HelpFormatter = new HelpFormatter();
    formatter.printHelp(80, syntax, header, options, footer);
  }

  def main(args: Array[String]): Unit = {
    val options: Options = new Options()
    val parser: CommandLineParser = new DefaultParser()
    options.addOption("i", true, "Input dataset")
    options.addOption("d", false, "Indicates that the dataset is a directed graph (defaults to undirected)")
    options.addOption("o", true, "Output path (defaults to a timestamped name)")
    options.addOption("s", true, "Minimum support threshold: patterns occurring more than this threshold will be mined")
    options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count minus one)")
    options.addOption("p", true, "How many partitions for the execution (defaults to 1)")
    options.addOption("n", true, "Nodes participating in the execution (IPs separated by \":\", must contain at least current node)")
    options.addOption("l", true, "Max pattern length in number of edges (defaults to 100)")
    options.addOption("b", false, "Benchmark mode: patterns are not outputted at all (in which case OUTPUT_PATH is ignored)")
    options.addOption("v", false, "Parallel validation (defaults to false)")
    options.addOption("e", true, "Print collected pattern count every (defaults to 100)")
    options.addOption("h", false, "Print help")

    val cmd: CommandLine = try {
      val cmd: CommandLine = parser.parse(options, args);
      if (cmd.hasOption('h') || !cmd.hasOption("i") || !cmd.hasOption("s") || !cmd.hasOption("n")) {
        printMan(options);
        return
      }
      cmd
    } catch {
      case e: ParseException =>
        printMan(options)
        return
    }

    val dataPath = cmd.getOptionValue("i")
    val minSupport = cmd.getOptionValue("s").toInt
    val numPartitions = cmd.getOptionValue("p", "1").toInt
    val maxPatternLength = cmd.getOptionValue("l", "100").toInt
    val directed = cmd.hasOption("d")
    val benchmark = cmd.hasOption("b")
    val parallelValidation = cmd.hasOption("v")
    val maxThreads = cmd.getOptionValue("t", Math.max(Runtime.getRuntime.availableProcessors - 1, 1).toString()).toInt
    val outputPath = cmd.getOptionValue("o", "StealSpanRunner_output_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date()))
    val nodeString = cmd.getOptionValue("n")
    val dispEvery = cmd.getOptionValue("e", "100").toInt

    println("dataPath: " + dataPath)
    println("minSupport: " + minSupport)
    println("numPartitions: " + numPartitions)
    println("maxPatternLength: " + maxPatternLength)
    println("directed: " + directed)
    println("maxThreads: " + maxThreads)
    println("parallel validation: " + parallelValidation)
    println("outputPath: " + outputPath)
    println("benchmark: " + benchmark)
    println("nodes: " + nodeString)

    println("Running: StealSpanRunner")

    var sw = new StopWatch()
    sw.start()

    println("reading node file")
    var nodeMap = ConfigLoader.getNodeMap(nodeString)
    if (numPartitions != nodeMap.size)
      throw new Exception("only 1:1 node to partition mapping supported")

    println("loading data file")
    var inputGraphAsMap = new GraphLoader().loadAsEdgeDag(dataPath, directed, minSupport, parallelValidation)
    println("Starting StealSpanRunner, elapsed time: " + sw.getElapsedTime() + "msecs")

    val freqPatterns = new Sami(inputGraphAsMap, minSupport, numPartitions, nodeMap, maxPatternLength, maxThreads, dispEvery).run
    if (!benchmark) {
      for (p <- freqPatterns) {
        println(p)
      }
    }

    println(InetAddress.getLocalHost().getHostAddress() + "(" + InetAddress.getLocalHost().getHostName() + ") final pattern count: " + freqPatterns.size)

    sw.stop()
    println("Finished: StealSpanRunner, Runtime: " + sw.getElapsedTime() + " msecs")
    System.exit(0)
  }

}
