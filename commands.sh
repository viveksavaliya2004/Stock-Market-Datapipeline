# Create Virtual Environment
py -3.10 -m venv venv

# Activate Virtual Environment
source venv/Scripts/activate

# Consumer-Side
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:29092 \
    --topic stock-market-realtime \
    --from-beginning

docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:29092

# Spark-Jars
org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901

# Check Spark Running
docker exec \
  -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin \
  -e MINIO_BUCKET=stock-market-data \
  -e MINIO_CONNECTION=http://minio:9000 \
  stockmarket_datapipeline-spark-master-1 \
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
    //opt/spark/jobs/spark_batch_processor.py