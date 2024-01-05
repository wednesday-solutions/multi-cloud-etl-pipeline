docker run -it -v <your_aws_credentials_locaiton>:/home/glue_user/.aws -v <project_root_location>:/home/glue_user/workspace/ -e AWS_PROFILE=default -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01

export KAGGLE_KEY=MOCKKEY
export KAGGLE_USERNAME=MOCKEUSERNAME
pip3 install -r requirements.txt
