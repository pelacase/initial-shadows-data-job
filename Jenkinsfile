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
                    sh 'apt-get install python3-pip'
                    sh 'python3 -m pip install -r requirements.txt'
                    sh 'python3 main.py'
                    sh 'aws s3 sync ./output s3://pelabenbucket'
                }
            }
        }
    }
}