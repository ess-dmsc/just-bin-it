@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "just-bin-it"

clangformat_os = "debian9"
test_and_coverage_os = "centos7"
release_os = "centos7-release"

container_build_nodes = [
  'centos7': new ContainerBuildNode('essdmscdm/centos7-build-node:3.2.0', '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash -e'),
  'centos7-release': new ContainerBuildNode('essdmscdm/centos7-build-node:3.2.0', '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash -e'),
  'debian9': new ContainerBuildNode('essdmscdm/debian9-build-node:2.5.2', 'bash -e'),
  'ubuntu1804': new ContainerBuildNode('essdmscdm/ubuntu18.04-build-node:1.2.0', 'bash -e')
]

// Define number of old builds to keep.
num_artifacts_to_keep = '1'


// Set number of old builds to keep.
properties([[
  $class: 'BuildDiscarderProperty',
  strategy: [
    $class: 'LogRotator',
    artifactDaysToKeepStr: '',
    artifactNumToKeepStr: num_artifacts_to_keep,
    daysToKeepStr: '',
    numToKeepStr: num_artifacts_to_keep
  ]
]]);


pipeline_builder = new PipelineBuilder(this, container_build_nodes)
pipeline_builder.activateEmailFailureNotifications()

builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: Checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage

  pipeline_builder.stage("${container.key}: Dependencies") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      pip install --upgrade pip
      pip install -r just-bin-it/requirements.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    container.sh """
      pytest --junitxml=results.xml
      junit 'results.xml'
    """
  } // stage

}  // createBuilders

node {
  dir("${project}") {
    try {
      scm_vars = checkout scm
    } catch (e) {
      failure_function(e, 'Checkout failed')
    }
  }

  try {
    parallel builders
  } catch (e) {
    pipeline_builder.handleFailureMessages()
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}
