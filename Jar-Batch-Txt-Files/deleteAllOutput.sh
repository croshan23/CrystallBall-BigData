echo ">> Deleting duplicate files if exists"
echo ">> Please wait..."

hadoop fs -rm /user/cloudera/Pair_output/*
hadoop fs -rm /user/cloudera/Stripes_output/*
hadoop fs -rm /user/cloudera/Hybrid_output/*

hadoop fs -rmdir /user/cloudera/Pair_output
hadoop fs -rmdir /user/cloudera/Stripes_output
hadoop fs -rmdir /user/cloudera/Hybrid_output

echo ">> Cleanup done..."
