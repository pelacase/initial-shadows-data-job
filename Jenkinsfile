pipeline {
    triggers {
        githubPush()
    }
    agent {
        node {
            label 'debian-awscli'
        }
    }

    stages {
        stage('Run Script') {
            steps {
                script {
                    sh 'python3 main.py'
                    sh 'aws s3 sync ./output s3://pelabenbucket'
                }
            }
        }
    }
}