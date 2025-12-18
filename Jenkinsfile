pipeline {
agent any

 environment {
        PYSPARK_PYTHON        = "/usr/bin/python3"
        PYSPARK_DRIVER_PYTHON = "/usr/bin/python3"
        SPARK_SUBMIT          = "spark-submit"
    }

stages {
stage('Checkout') {
steps {
git branch: 'main', url: 'https://github.com/RupaliRW/dataqualityGE.git'
}
}


stage('Install Dependencies') {
steps {
sh '''
which -a python
which -a python3
which -a python3.10
python3 -m pip install --upgrade pip setuptools wheel
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
'''
}
}


stage('Run Spark + GE Pipeline') {
steps {
sh '''
. venv/bin/activate
spark-submit spark/orders.py
'''
}
}
}


post {
failure {
echo 'Pipeline failed due to data quality threshold breach'
}
success {
echo 'Pipeline succeeded with acceptable data quality'
}
}
}
