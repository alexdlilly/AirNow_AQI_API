1. Must first configure your AWS CLI.
2. Then run `bash setup_infra.sh <your_bucket_name>`. Ensure that this bucket name meets AWS's naming requirements, and matches what's referenced in the lambda_function.py file.
3. Go into the AWS Lambda interface on the web portal and click Layers. Add the pandas layer.
