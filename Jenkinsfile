@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "just-bin-it"

container_build_nodes = [
  'centos7': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11')
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
    container.sh """
      which python
      python --version
      python -m pip install --user -r ${project}/requirements-dev.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    def test_output = "TestResults.xml"
    container.sh """
      cd ${project}
      pyenv global 3.8 3.9 3.10
      pyenv versions
      export PATH="/home/jenkins/.pyenv/shims:$PATH"
      python -m nox -- --junitxml=${test_output}
    """
    container.copyFrom("${project}/${test_output}", ".")
    xunit thresholds: [failed(unstableThreshold: '0')], tools: [JUnit(deleteOutputFiles: true, pattern: '*.xml', skipNoTestFiles: false, stopProcessingIfError: true)]
  } // stage
}  // createBuilders

node {
  dir("${project}") {
    scm_vars = checkout scm
  }

  if ( env.CHANGE_ID ) {
      builders['integration tests'] = get_integration_tests_pipeline()
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}

def get_integration_tests_pipeline() {
  return {
    node('docker') {
      cleanWs()
      dir("${project}") {
        try {
          stage("Integration tests: Checkout") {
            checkout scm
          }  // stage
          stage("Integration tests: Install requirements") {
            sh """
            scl enable rh-python38 -- which python
            scl enable rh-python38 -- python -m venv test_env
            source test_env/bin/activate
            which python
            pip install --upgrade pip
            pip install -r requirements-dev.txt
            pip install -r integration-tests/requirements.txt
            pip install 'requests<2.30.0'
            """
          }  // stage
          stage("Integration tests: Run") {
            // Stop and remove any containers that may have been from the job before,
            // i.e. if a Jenkins job has been aborted.
            sh "docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true"
            timeout(time: 30, activity: true){
              sh """
              source test_env/bin/activate
              cd integration-tests/
              python -m pytest -s --junitxml=./IntegrationTestsOutput.xml .
              """
            }
          }  // stage
        } finally {
          stage ("Integration tests: Clean Up") {
            // The statements below return true because the build should pass
            // even if there are no docker containers or output files to be
            // removed.
            sh """
            rm -rf test_env
            rm -rf integration-tests/output-files/* || true
            docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true
            """
          }  // stage
          stage("Integration tests: Archive") {
            junit "integration-tests/IntegrationTestsOutput.xml"
          }
        }  // try/finally
      } // dir
    }  // node
  }  // return
} // def
