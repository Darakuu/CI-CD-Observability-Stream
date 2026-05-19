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

  environment {
    SERVICE_NAME = 'demo-service'
    SERVICE_MODULE = 'demo-service-api'
    TARGET_ENVIRONMENT = 'staging'
    SOURCE_BRANCH = 'main'
  }

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
            'echo "event=stage_start stage=checkout service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER branch=$SOURCE_BRANCH"',
            'echo "event=checkout service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER repository=$SERVICE_NAME branch=$SOURCE_BRANCH"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=checkout service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER forced_success=true reason=tenth_build"',
            '  echo "event=checkout stage=checkout status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER repository=$SERVICE_NAME branch=$SOURCE_BRANCH commit_sha=demo-$BUILD_NUMBER"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=checkout service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER random_scenario=$scenario"',
            'case $scenario in',
            '  1)',
            '    echo "event=checkout stage=checkout status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER reason=scm_timeout detail=git_provider_response_time_exceeded_30s"',
            '    exit 11',
            '    ;;',
            'esac',
            'echo "event=checkout stage=checkout status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER repository=$SERVICE_NAME branch=$SOURCE_BRANCH commit_sha=demo-$BUILD_NUMBER"',
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
            'echo "event=stage_start stage=preflight service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER agent=$NODE_NAME"',
            'echo "event=preflight service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER workspace=$WORKSPACE executor=$EXECUTOR_NUMBER"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=preflight service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER forced_success=true reason=tenth_build"',
            '  echo "event=preflight stage=preflight status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER disk_free_pct=42 cpu_temp_c=58"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=preflight service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER random_scenario=$scenario"',
            'case $scenario in',
            '  2)',
            '    echo "event=preflight stage=preflight status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER reason=disk_full detail=workspace_volume_available_space_below_1_percent"',
            '    exit 21',
            '    ;;',
            'esac',
            'disk_free_pct=$((35 + scenario))',
            'cpu_temp_c=$((56 + scenario * 4))',
            'if [ "$cpu_temp_c" -gt 90 ]; then',
            '  echo "event=preflight stage=preflight status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER reason=thermal_throttling detail=agent_cpu_temperature_above_safe_limit disk_free_pct=$disk_free_pct cpu_temp_c=$cpu_temp_c"',
            '  exit 22',
            'fi',
            'echo "event=preflight stage=preflight status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER disk_free_pct=$disk_free_pct cpu_temp_c=$cpu_temp_c"',
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
            'echo "event=stage_start stage=build service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER module=$SERVICE_MODULE"',
            'echo "event=build service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER tool=maven module=$SERVICE_MODULE"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=build service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER forced_success=true reason=tenth_build"',
            '  echo "event=build stage=build status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER module=$SERVICE_MODULE compile_time_ms=4200 dependency_cache=hit"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=build service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER random_scenario=$scenario"',
            'case $scenario in',
            '  5)',
            '    echo "event=build stage=build status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER module=$SERVICE_MODULE reason=dependency_resolution detail=artifact_repository_returned_503"',
            '    exit 31',
            '    ;;',
            'esac',
            'compile_time_ms=$((3800 + scenario * 250))',
            'echo "event=build stage=build status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER module=$SERVICE_MODULE compile_time_ms=$compile_time_ms dependency_cache=hit"',
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
            'echo "event=stage_start stage=test service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER suite=unit"',
            'echo "event=test service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER suite=unit total=42"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=test service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER forced_success=true reason=tenth_build"',
            '  echo "event=test stage=test status=passed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER total=42 passed_tests=42 failing_tests=0 duration_ms=5100"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=test service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER random_scenario=$scenario"',
            'case $scenario in',
            '  7)',
            '    echo "event=test stage=test status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER reason=flaky_test failing_tests=1 error_code=UT-001"',
            '    exit 41',
            '    ;;',
            'esac',
            'duration_ms=$((4800 + scenario * 180))',
            'echo "event=test stage=test status=passed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER total=42 passed_tests=42 failing_tests=0 duration_ms=$duration_ms"',
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
            'echo "event=stage_start stage=package service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER"',
            'echo "event=package service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER artifact=$SERVICE_NAME.jar"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=package service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER forced_success=true reason=tenth_build"',
            '  echo "event=package stage=package status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER artifact=$SERVICE_NAME.jar artifact_size_mb=18 checksum=sha256:demo-$BUILD_NUMBER"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=package service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER random_scenario=$scenario"',
            'case $scenario in',
            '  8)',
            '    echo "event=package stage=package status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER reason=artifact_checksum_mismatch detail=jar_digest_changed_after_build"',
            '    exit 51',
            '    ;;',
            'esac',
            'artifact_size_mb=$((16 + scenario))',
            'echo "event=package stage=package status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER artifact=$SERVICE_NAME.jar artifact_size_mb=$artifact_size_mb checksum=sha256:demo-$BUILD_NUMBER"',
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
            'echo "event=stage_start stage=deploy service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER environment=$TARGET_ENVIRONMENT"',
            'echo "event=deploy service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER environment=$TARGET_ENVIRONMENT strategy=rolling"',
            'if [ $((BUILD_NUMBER % 10)) -eq 0 ] && [ "$BUILD_NUMBER" -ne 0 ]; then',
            '  echo "event=simulation stage=deploy service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER forced_success=true reason=tenth_build"',
            '  echo "event=deploy stage=deploy status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER environment=$TARGET_ENVIRONMENT replicas_ready=3 replicas_expected=3 rollout_seconds=35"',
            '  sleep 1',
            '  exit 0',
            'fi',
            'scenario=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d " ") % 12 ))',
            'echo "event=simulation stage=deploy service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER random_scenario=$scenario"',
            'case $scenario in',
            '  10)',
            '    echo "event=deploy stage=deploy status=failed service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER environment=$TARGET_ENVIRONMENT reason=rollout_timeout detail=staging_pods_not_ready_after_120s"',
            '    exit 61',
            '    ;;',
            'esac',
            'rollout_seconds=$((30 + scenario * 2))',
            'echo "event=deploy stage=deploy status=success service=$SERVICE_NAME job_name=$JOB_NAME build_number=$BUILD_NUMBER environment=$TARGET_ENVIRONMENT replicas_ready=3 replicas_expected=3 rollout_seconds=$rollout_seconds"',
            'sleep 1'
          ].join('\\n')
        )
      }
    }
  }

  post {
    success {
      echo "event=pipeline_result stage=pipeline status=success pipeline_status=success job_name=${env.JOB_NAME} build_number=${env.BUILD_NUMBER}"
    }
    failure {
      echo "event=pipeline_result stage=pipeline status=failed pipeline_status=failure job_name=${env.JOB_NAME} build_number=${env.BUILD_NUMBER}"
    }
    always {
      echo "event=build_summary stage=pipeline status=${currentBuild.currentResult == 'SUCCESS' ? 'success' : 'failed'} build_url=${env.BUILD_URL} job_name=${env.JOB_NAME} build_number=${env.BUILD_NUMBER}"
    }
  }
}
      '''.stripIndent())
    }
  }
}
