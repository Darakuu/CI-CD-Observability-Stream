pipelineJob('demo-ci-observability') {
  description('Demo CI/CD pipeline instrumentata tramite Jenkins OpenTelemetry plugin.')

  // The job is intentionally small: it creates enough activity to produce CI telemetry.
  // Demo behavior:
  // - Each build simulates a normal CI/CD flow for a small service.
  // - Most runs may fail randomly at one stage, using realistic CI failure causes.
  // - Every 10th build is ALWAYS SUCCESSFUL, so the final dataset always has successful runs too.
  definition {
    cps {
      sandbox()
      script('''
pipeline {
  agent any

  options {
    timestamps()
    buildDiscarder(logRotator(numToKeepStr: '30'))
  }

  stages {
    stage('Checkout') {
      steps {
        // Checkout simulates getting source code from Git.
        // Success means the branch was fetched; failure means the SCM provider timed out.
        sh(
          label: 'Simulate source checkout',
          script: [
            'echo "event=checkout repository=demo-service branch=main"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=checkout forced_success=true reason=tenth_build"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=checkout random_scenario=$scenario"',
            'case $scenario in',
            '  1)',
            '    echo "event=checkout status=failed reason=scm_timeout detail=git_provider_response_time_exceeded_30s"',
            '    exit 11',
            '    ;;',
            'esac',
            'sleep 1'
          ].join('\\n')
        )
      }
    }

    stage('Pre-flight Checks') {
      steps {
        // Pre-flight checks simulate basic health checks on the Jenkins agent.
        // Success means the agent is usable; failures mean disk pressure or CPU throttling.
        sh(
          label: 'Check build agent health',
          script: [
            'echo "event=preflight workspace=$WORKSPACE executor=$EXECUTOR_NUMBER"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=preflight forced_success=true reason=tenth_build"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=preflight random_scenario=$scenario"',
            'case $scenario in',
            '  2)',
            '    echo "event=preflight status=failed reason=disk_full detail=workspace_volume_available_space_below_1_percent"',
            '    exit 21',
            '    ;;',
            '  4)',
            '    echo "event=preflight status=failed reason=thermal_throttling detail=agent_cpu_temperature_above_safe_limit"',
            '    exit 22',
            '    ;;',
            'esac',
            'sleep 1'
          ].join('\\n')
        )
      }
    }

    stage('Build') {
      steps {
        // Build simulates compiling the service and resolving dependencies.
        // Success means compilation can continue; failure means the artifact repository is unavailable.
        sh(
          label: 'Compile demo service',
          script: [
            'echo "event=build tool=maven module=demo-service"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=build forced_success=true reason=tenth_build"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=build random_scenario=$scenario"',
            'case $scenario in',
            '  5)',
            '    echo "event=build status=failed reason=dependency_resolution detail=artifact_repository_returned_503"',
            '    exit 31',
            '    ;;',
            'esac',
            'sleep 1'
          ].join('\\n')
        )
      }
    }

    stage('Unit Tests') {
      steps {
        // Unit Tests simulate a small automated test suite.
        // Success means all tests pass; failure means a flaky test broke this run.
        sh(
          label: 'Run unit tests',
          script: [
            'echo "event=test suite=unit total=42"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=test forced_success=true reason=tenth_build"',
            '  echo "event=test status=passed failing_tests=0"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=test random_scenario=$scenario"',
            'case $scenario in',
            '  7)',
            '    echo "event=test status=failed reason=flaky_test failing_tests=1 error_code=UT-001"',
            '    exit 41',
            '    ;;',
            'esac',
            'echo "event=test status=passed failing_tests=0"',
            'sleep 1'
          ].join('\\n')
        )
      }
    }

    stage('Package') {
      steps {
        // Package simulates creating the deployable artifact.
        // Success means the artifact is valid; failure means its checksum does not match.
        sh(
          label: 'Package artifact',
          script: [
            'echo "event=package artifact=demo-service.jar"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=package forced_success=true reason=tenth_build"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=package random_scenario=$scenario"',
            'case $scenario in',
            '  8)',
            '    echo "event=package status=failed reason=artifact_checksum_mismatch detail=jar_digest_changed_after_build"',
            '    exit 51',
            '    ;;',
            'esac',
            'sleep 1'
          ].join('\\n')
        )
      }
    }

    stage('Deploy Staging') {
      steps {
        // Deploy Staging simulates a rolling deployment to a staging environment.
        // Success means the rollout completed; failure means the new pods did not become ready.
        sh(
          label: 'Deploy to staging',
          script: [
            'echo "event=deploy environment=staging strategy=rolling"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=deploy forced_success=true reason=tenth_build"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=deploy random_scenario=$scenario"',
            'case $scenario in',
            '  10)',
            '    echo "event=deploy status=failed reason=rollout_timeout detail=staging_pods_not_ready_after_120s"',
            '    exit 61',
            '    ;;',
            'esac',
            'sleep 1'
          ].join('\\n')
        )
      }
    }
  }

  post {
    success {
      echo 'pipeline_status=success'
    }
    failure {
      echo 'pipeline_status=failure'
    }
    always {
      echo "build_url=${env.BUILD_URL} job_name=${env.JOB_NAME} build_number=${env.BUILD_NUMBER}"
    }
  }
}
      '''.stripIndent())
    }
  }
}
