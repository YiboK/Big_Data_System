hdfs namenode -format -force
hdfs namenode -D dfs.webhdfs.enabled=true -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheck-interval=30000 -fs hdfs://main:9000 &
cd /notebooks
python3 -m jupyterlab --no-browser --ip=0.0.0.0 --port=5000 --allow-root --NotebookApp.token=''