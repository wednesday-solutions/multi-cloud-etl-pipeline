# Parameter 1 --> Shell profile path
SOURCE_FILE=$1
echo $SOURCE_FILE

echo -e "FIRST RUN TIME ESTIMATION: 30-45 MINS\nPlease do NOT exit"

export PROJECT_ROOT=$(pwd)

# Doing all the work in separate folder "glue-libs"
cd ~
mkdir glue-libs
cd glue-libs

# Clone AWS Glue Python Lib
git clone https://github.com/awslabs/aws-glue-libs.git
export AWS_GLUE_HOME=$(pwd)/aws-glue-libs

# Install Apache Maven
curl https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz -o apache-maven-3.6.0-bin.tar.gz
tar -xvf apache-maven-3.6.0-bin.tar.gz
ln -s apache-maven-3.6.0 maven
export MAVEN_HOME=$(pwd)/maven

# Install Apache Spark
curl https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-3.0/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3.tgz -o spark-3.1.1-amzn-0-bin-3.2.1-amzn-3.tgz
tar -xvf spark-3.1.1-amzn-0-bin-3.2.1-amzn-3.tgz
ln -s spark-3.1.1-amzn-0-bin-3.2.1-amzn-3 spark
export SPARK_HOME=$(pwd)/spark

# Export Path
export PATH=$PATH:$SPARK_HOME/bin:$MAVEN_HOME/bin:$AWS_GLUE_HOME/bin
export PYTHONPATH=$PROJECT_ROOT

# Download Glue ETL .jar files
cd $AWS_GLUE_HOME
chmod +x bin/glue-setup.sh
./bin/glue-setup.sh
mvn install dependency:copy-dependencies
cp $AWS_GLUE_HOME/jarsv1/AWSGlue*.jar $SPARK_HOME/jars/
cp $AWS_GLUE_HOME/jarsv1/aws*.jar $SPARK_HOME/jars/

echo "export AWS_GLUE_HOME=$AWS_GLUE_HOME
export MAVEN_HOME=$MAVEN_HOME
export SPARK_HOME=$SPARK_HOME
export PATH=$PATH:$SPARK_HOME/bin:$MAVEN_HOME/bin:$AWS_GLUE_HOME/bin
export PYTHONPATH=$PROJECT_ROOT" >> $SOURCE_FILE


cd $PROJECT_ROOT

echo -e "\nGLUE LOCAL SETUP COMPLETE"
