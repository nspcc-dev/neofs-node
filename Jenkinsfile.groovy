@Library('Github') _

pipeline {
    agent { label 'slave' }
    stages {
        stage('Trigger neofs-node CI') {
            steps {
                build job: 'neofs_node_ci', parameters: [string(name: 'branch', value: env.GIT_BRANCH)], wait: true
            }
        }
    }
    post {
        always {
            script {
                def g = new org.nspcc.Github()
                g.reportGHStatus("nspcc-dev/neofs-node")
            }
        }
    }
}
