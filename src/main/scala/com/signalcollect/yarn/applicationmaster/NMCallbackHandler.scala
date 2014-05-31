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

import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import com.signalcollect.util.LogHelper
import java.nio.ByteBuffer
import org.apache.hadoop.yarn.api.records.ContainerState
import com.signalcollect.util.ConfigProvider

class NMCallbackHandler
  extends NMClientAsync.CallbackHandler with LogHelper {
  override def onContainerStopped(containerId: ContainerId) {
    log.info("onContainerStopped")
  }

  override def onContainerStatusReceived(containerId: ContainerId,
    containerStatus: ContainerStatus) {
    log.info("onOntainerStatusReceived")
    if (ContainerState.COMPLETE.equals(containerStatus.getState())) {
      if (containerStatus.getExitStatus() != 0) {
        ContainerRegistry.setSuccessfull(false)
      }
    }
  }

  override def onContainerStarted(containerId: ContainerId,
    allServiceResponse: java.util.Map[String, ByteBuffer]) {
    log.info("onContainerStarted")
    ContainerRegistry.containerStarted()
  }

  override def onStartContainerError(containerId: ContainerId, t: Throwable) {
    log.info("onStartContainerError")
    ContainerRegistry.setSuccessfull(false)
  }

  override def onGetContainerStatusError(
    containerId: ContainerId, t: Throwable) {
    log.error("Failed to query the status of Container " + containerId);
    ContainerRegistry.setSuccessfull(false)
  }

  override def onStopContainerError(containerId: ContainerId, t: Throwable) {
    log.error("Failed to stop Container " + containerId)
    ContainerRegistry.setSuccessfull(false)
  }
}
