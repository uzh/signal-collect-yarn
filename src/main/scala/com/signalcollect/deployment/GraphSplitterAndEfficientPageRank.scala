package com.signalcollect.deployment

import akka.actor.ActorRef

class GraphSplitterAndEfficientPageRank extends YarnDeployableAlgorithm {
  def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
    println("start executing GraphSplitterAndEfficientPageRank")
	DeployableGraphSplitter.execute(parameters, nodeActors)
    DeployableEfficientPageRankLoader.execute(parameters, nodeActors)

  }
}