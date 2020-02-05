@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "just-bin-it"

container_build_nodes = [
  'ubuntu1804': ContainerBuildNode.getDefaultContainerBuildNode('ubuntu1804')
]

python = 'python3.8'

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
      apt update
      apt install -yq wget git software-properties-common curl apt-transport-https ca-certificates gnupg-agent
      add-apt-repository -y ppa:deadsnakes/ppa
      apt install -yq ${python}
      curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && ${python} get-pip.py
      ${python} -m pip install -r ${project}/requirements.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    def test_output = "TestResults.xml"
    container.sh """
      ${python} --version
      cd ${project}
      ${python} -m pytest --junitxml=${test_output}
    """
    container.copyFrom("${project}/${test_output}", ".")
    junit "${test_output}"

  } // stage

  pipeline_builder.stage("${container.key}: System Tests") {
    def test_output = "SystemTestResults.xml"
    container.sh """
      ${python} --version
      cd ${project}/system-tests
      docker-compose up &
      ${python} ../bin/just-bin-it.py -b localhost:9092 -t hist_commands &
      ${python} -m pytest --junitxml=${test_output}
    """
    container.copyFrom("${project}/${test_output}", ".")
    junit "${test_output}"

  } // stage

}  // createBuilders

node {
  dir("${project}") {
    scm_vars = checkout scm
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}
