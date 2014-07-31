/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.signalcollect.yarn.applicationmaster

import java.net.InetAddress

import org.apache.hadoop.yarn.api.records.Container

import com.signalcollect.deployment.DeploymentConfigurationCreator
/**
 * keeps track of the running containers
 */
object ContainerRegistry {
  val deploymentConfig = DeploymentConfigurationCreator.getDeploymentConfiguration
  var containers = Map[String, (Container, Int)]()
  var counter = 0
  var finished = false
  var successfull = true
  var finishedCounter = 0
  var startedCounter = 0

  def register(container: Container): Int = {
    synchronized {
      val containerId = container.getId.toString
      val value = (container, counter)
      containers += ((containerId, value))
      counter += 1
      value._2
    }
  }

  def contains(container: Container): Boolean = {
    synchronized {
      containers.contains(container.getId.toString)
    }
  }

  def remove(container: Container) {
    synchronized {
      containers -= container.getId.toString
    }
  }

  def retrieve(containerId: String): Option[(Container, Int)] = {
    synchronized {
      containers.get(containerId)
    }
  }

  def setSuccessfull(isSuccessfull: Boolean) {
    synchronized {
      successfull = successfull && isSuccessfull
    }
  }

  def setFinished(numberOfFinishedContainers: Int) {
    synchronized {
      finishedCounter += numberOfFinishedContainers
    }
  }

  def isFinished: Boolean = {
    synchronized {
      finishedCounter == deploymentConfig.numberOfNodes 
    }
  }

  def containerStarted {
    synchronized {
      startedCounter += 1
    }
  }
  
  def allStarted: Boolean = {
    synchronized {
      startedCounter == deploymentConfig.numberOfNodes 
    }
  }
  
  def failContainers {
    synchronized {
      successfull = false
      finished = true
    }
  }
}