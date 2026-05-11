pipelineJob('demo-ci-observability') {
  description('Demo CI/CD pipeline instrumentata tramite Jenkins OpenTelemetry plugin.')

  // The job is intentionally small: it creates enough activity to produce CI telemetry.
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
        // The demo avoids a real repository dependency while still producing stage spans.
        sh(label: 'Simulate source checkout', script: 'echo "event=checkout repository=demo-service branch=main"; sleep 1')
      }
    }

    stage('Build') {
      steps {
        sh(label: 'Compile demo service', script: 'echo "event=build tool=maven module=demo-service"; sleep 1')
      }
    }

    stage('Unit Tests') {
      steps {
        sh(
          label: 'Run unit tests',
          script: [
            'echo "event=test suite=unit total=42"',
            'if [ $(( $(date +%s) % 4 )) -eq 0 ]; then',
            '  echo "event=test status=failed failing_tests=1 error_code=UT-001"',
            '  exit 1',
            'fi',
            'echo "event=test status=passed failing_tests=0"',
            'sleep 1'
          ].join('\\n')
        )
      }
    }

    stage('Package') {
      steps {
        sh(label: 'Package artifact', script: 'echo "event=package artifact=demo-service.jar"; sleep 1')
      }
    }

    stage('Deploy Staging') {
      steps {
        sh(label: 'Deploy to staging', script: 'echo "event=deploy environment=staging strategy=rolling"; sleep 1')
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
