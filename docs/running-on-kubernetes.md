---
layout: global
title: Running Spark on Kubernetes
---

Support for running on [Kubernetes](https://kubernetes.io/docs/whatisk8s/) is available in experimental status. The feature set is
currently limited and not well-tested. This should not be used in production environments.

## Prerequisites

* You must have a running Kubernetes cluster with access configured to it using [kubectl](https://kubernetes.io/docs/user-guide/prereqs/). If you do not already have a working Kubernetes cluster, you may setup a test cluster on your local machine using [minikube](https://kubernetes.io/docs/getting-started-guides/minikube/).
* You must have appropriate permissions to create and list [pods](https://kubernetes.io/docs/user-guide/pods/), [nodes](https://kubernetes.io/docs/admin/node/) and [services](https://kubernetes.io/docs/user-guide/services/) in your cluster. You can verify that you can list these resources by running `kubectl get nodes`, `kubectl get pods` and `kubectl get svc` which should give you a list of nodes, pods and services (if any) respectively.
* You must have an extracted spark distribution with Kubernetes support, or build one from [source](https://github.com/apache-spark-on-k8s/spark).

## Setting Up Docker Images

Kubernetes requires users to supply images that can be deployed into containers within pods. The images are built to
be run in a container runtime environment that Kubernetes supports. Docker is a container runtime environment that is
frequently used with Kubernetes, so Spark provides some support for working with Docker to get started quickly.

To use Spark on Kubernetes with Docker, images for the driver and the executors need to built and published to an
accessible Docker registry. Spark distributions include the Docker files for the driver and the executor at
`dockerfiles/driver/Dockerfile` and `docker/executor/Dockerfile`, respectively. Use these Docker files to build the
Docker images, and then tag them with the registry that the images should be sent to. Finally, push the images to the
registry.

For example, if the registry host is `registry-host` and the registry is listening on port 5000:

    cd $SPARK_HOME
    docker build -t registry-host:5000/spark-driver:latest -f dockerfiles/driver/Dockerfile .
    docker build -t registry-host:5000/spark-executor:latest -f dockerfiles/executor/Dockerfile .
    docker push registry-host:5000/spark-driver:latest
    docker push registry-host:5000/spark-executor:latest
    
## Submitting Applications to Kubernetes

Kubernetes applications can be executed via `spark-submit`. For example, to compute the value of pi, assuming the images
are set up as described above:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      examples/jars/spark_examples_2.11-2.2.0.jar

The Spark master, specified either via passing the `--master` command line argument to `spark-submit` or by setting
`spark.master` in the application's configuration, must be a URL with the format `k8s://<api_server_url>`. Prefixing the
master string with `k8s://` will cause the Spark application to launch on the Kubernetes cluster, with the API server
being contacted at `api_server_url`. If no HTTP protocol is specified in the URL, it defaults to `https`. For example,
setting the master to `k8s://example.com:443` is equivalent to setting it to `k8s://https://example.com:443`, but to
connect without SSL on a different port, the master would be set to `k8s://http://example.com:8443`.

Note that applications can currently only be executed in cluster mode, where the driver and its executors are running on
the cluster.
 
### Dependency Management and Docker Containers

Spark supports specifying JAR paths that are either on the submitting host's disk, or are located on the disk of the
driver and executors. Refer to the [application submission](submitting-applications.html#advanced-dependency-management)
section for details. Note that files specified with the `local` scheme should be added to the container image of both
the driver and the executors. Files without a scheme or with the scheme `file://` are treated as being on the disk of
the submitting machine, and are uploaded to the driver running in Kubernetes before launching the application.
 
### Setting Up SSL For Submitting the Driver

When submitting to Kubernetes, a pod is started for the driver, and the pod starts an HTTP server. This HTTP server
receives the driver's configuration, including uploaded driver jars, from the client before starting the application.
Spark supports using SSL to encrypt the traffic in this bootstrapping process. It is recommended to configure this
whenever possible. 

See the [security page](security.html) and [configuration](configuration.html) sections for more information on
configuring SSL; use the prefix `spark.ssl.kubernetes.submit` in configuring the SSL-related fields in the context
of submitting to Kubernetes. For example, to set the trustStore used when the local machine communicates with the driver
pod in starting the application, set `spark.ssl.kubernetes.submit.trustStore`.

One note about the keyStore is that it can be specified as either a file on the client machine or a file in the
container image's disk. Thus `spark.ssl.kubernetes.submit.keyStore` can be a URI with a scheme of either `file:`
or `local:`. A scheme of `file:` corresponds to the keyStore being located on the client machine; it is mounted onto
the driver container as a [secret volume](https://kubernetes.io/docs/user-guide/secrets/). When the URI has the scheme
`local:`, the file is assumed to already be on the container's disk at the appropriate path.

### Kubernetes Clusters and the authenticated proxy endpoint

Spark-submit also supports submission through the
[local kubectl proxy](https://kubernetes.io/docs/user-guide/accessing-the-cluster/#using-kubectl-proxy). One can use the
authenticating proxy to communicate with the api server directly without passing credentials to spark-submit.

The local proxy can be started by running:

    kubectl proxy

If our local proxy were listening on port 8001, we would have our submission looking like the following:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://http://127.0.0.1:8001 \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      examples/jars/spark_examples_2.11-2.2.0.jar

Communication between Spark and Kubernetes clusters is performed using the fabric8 kubernetes-client library.
The above mechanism using `kubectl proxy` can be used when we have authentication providers that the fabric8
kubernetes-client library does not support. Authentication using X509 Client Certs and oauth tokens
is currently supported.

### Determining the Driver Base URI

Kubernetes pods run with their own IP address space. If Spark is run in cluster mode, the driver pod may not be
accessible to the submitter. However, the submitter needs to send local dependencies from its local disk to the driver
pod.

By default, Spark will place a [Service](https://kubernetes.io/docs/user-guide/services/#type-nodeport) with a NodePort
that is opened on every node. The submission client will then contact the driver at one of the node's
addresses with the appropriate service port.

There may be cases where the nodes cannot be reached by the submission client. For example, the cluster may
only be reachable through an external load balancer. The user may provide their own external URI for Spark driver
services. To use a your own external URI instead of a node's IP and node port, first set
`spark.kubernetes.driver.serviceManagerType` to `ExternalAnnotation`. A service will be created with the annotation
`spark-job.alpha.apache.org/provideExternalUri`, and this service routes to the driver pod. You will need to run a
separate process that watches the API server for services that are created with this annotation in the application's
namespace (set by `spark.kubernetes.namespace`). The process should determine a URI that routes to this service
(potentially configuring infrastructure to handle the URI behind the scenes), and patch the service to include an
annotation `spark-job.alpha.apache.org/resolvedExternalUri`, which has its value as the external URI that your process
has provided (e.g. `https://example.com:8080/my-job`).

Note that the URI provided in the annotation needs to route traffic to the appropriate destination on the pod, which has
a empty path portion of the URI. This means the external URI provider will likely need to rewrite the path from the
external URI to the destination on the pod, e.g. https://example.com:8080/spark-app-1/submit will need to route traffic
to https://<pod_ip>:<service_port>/. Note that the paths of these two URLs are different.

If the above is confusing, keep in mind that this functionality is only necessary if the submitter cannot reach any of
the nodes at the driver's node port. It is recommended to use the default configuration with the node port service
whenever possible.

### Spark Properties

Below are some other common properties that are specific to Kubernetes. Most of the other configurations are the same
from the other deployment modes. See the [configuration page](configuration.html) for more information on those.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.kubernetes.namespace</code></td>
  <td><code>default</code></td>
  <td>
    The namespace that will be used for running the driver and executor pods. When using
    <code>spark-submit</code> in cluster mode, this can also be passed to <code>spark-submit</code> via the
    <code>--kubernetes-namespace</code> command line argument.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.docker.image</code></td>
  <td><code>spark-driver:2.2.0</code></td>
  <td>
    Docker image to use for the driver. Specify this using the standard
    <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.docker.image</code></td>
  <td><code>spark-executor:2.2.0</code></td>
  <td>
    Docker image to use for the executors. Specify this using the standard
    <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.caCertFile</code></td>
  <td>(none)</td>
  <td>
    CA cert file for connecting to Kubernetes over SSL. This file should be located on the submitting machine's disk.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Client key file for authenticating against the Kubernetes API server. This file should be located on the submitting
    machine's disk.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Client cert file for authenticating against the Kubernetes API server. This file should be located on the submitting
    machine's disk.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.serviceAccountName</code></td>
  <td><code>default</code></td>
  <td>
    Service account that is used when running the driver pod. The driver pod uses this service account when requesting
    executor pods from the API server.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.memoryOverhead</code></td>
  <td>executorMemory * 0.10, with minimum of 384</td>
  <td>
    The amount of off-heap memory (in megabytes) to be allocated per executor. This is memory that accounts for things
    like VM overheads, interned strings, other native overheads, etc. This tends to grow with the executor size
    (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.submissionServerMemory</code></td>
  <td>256m</td>
  <td>
    The amount of memory to allocate for the driver submission server.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.memoryOverhead</code></td>
  <td>(driverMemory + driverSubmissionServerMemory) * 0.10, with minimum of 384</td>
  <td>
    The amount of off-heap memory (in megabytes) to be allocated for the driver and the driver submission server. This
    is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to
    grow with the driver size (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.labels</code></td>
  <td>(none)</td>
  <td>
    Custom labels that will be added to the driver pod. This should be a comma-separated list of label key-value pairs,
    where each label is in the format <code>key=value</code>. Note that Spark also adds its own labels to the driver pod
    for bookkeeping purposes.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.annotations</code></td>
  <td>(none)</td>
  <td>
    Custom annotations that will be added to the driver pod. This should be a comma-separated list of label key-value
    pairs, where each annotation is in the format <code>key=value</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driverSubmitTimeout</code></td>
  <td>60s</td>
  <td>
    Time to wait for the driver pod to start running before aborting its execution.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.service.exposeUiPort</code></td>
  <td><code>false</code></td>
  <td>
    Whether to expose the driver Web UI port as a service NodePort. Turned off by default because NodePort is a limited
    resource.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.waitAppCompletion</code></td>
  <td><code>true</code></td>
  <td>
    In cluster mode, whether to wait for the application to finish before exiting the launcher process.  When changed to
    false, the launcher has a "fire-and-forget" behavior when launching the Spark job.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.report.interval</code></td>
  <td><code>1s</code></td>
  <td>
    Interval between reports of the current Spark job status in cluster mode.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.serviceManagerType</code></td>
  <td><code>NodePort</code></td>
  <td>
    A tag indicating which class to use for creating the Kubernetes service and determining its URI for the submission
    client. Valid values are currently <code>NodePort</code> and <code>ExternalAnnotation</code>. By default, a service
    is created with the <code>NodePort</code> type, and the driver will be contacted at one of the nodes at the port
    that the nodes expose for the service. If the nodes cannot be contacted from the submitter's machine, consider
    setting this to <code>ExternalAnnotation</code> as described in "Determining the Driver Base URI" above. One may
    also include a custom implementation of <code>org.apache.spark.deploy.rest.kubernetes.DriverServiceManager</code> on
    the submitter's classpath - spark-submit service loads an instance of that class. To use the custom
    implementation, set this value to the custom implementation's return value of 
    <code>DriverServiceManager#getServiceManagerType()</code>. This method should only be done as a last resort.
  </td>
</tr>
</table>

## Current Limitations

Running Spark on Kubernetes is currently an experimental feature. Some restrictions on the current implementation that
should be lifted in the future include:
* Applications can only use a fixed number of executors. Dynamic allocation is not supported.
* Applications can only run in cluster mode.
* Only Scala and Java applications can be run.
