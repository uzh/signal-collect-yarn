package com.signalcollect.util

import com.typesafe.config.ConfigFactory

object ConfigProvider {
  //change name of config you use here
  val config = ConfigFactory.load("test-deployment")
}