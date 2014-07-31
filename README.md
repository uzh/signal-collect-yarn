signal-collect-yarn
===================

Support for Signal/Collect YARN deployment.


To use it check out the master and then

cd signal-collect-yarn
sbt assembly
sbt eclipse

You can now run the object com.signalcollect.deployment.PageRankYarnExample as a Scala Application.
This starts the ``MiniCluster'' and executes this simple algorithm.


If this works, you can try to deploy it to your own Hadoop YARN Cluster.
For this you have to change the parameters in the file deployment.conf.
The most important parameters, you have to change are: hdfspath, user
all the hadoop-overrides, and useMiniCluster.
When you changed these parameters, you can start again the object com.signalcollect.deploymentPageRankYarnExample.
Now it should connect to your cluster and deploy the algorithm.


If you want to use the ``AmazonCluster'', change in the deployment.conf the parameter cluster to com.signalcollect.deployment.amazon.AmazonCluster
and fill in the parameters in the amazon.conf.
