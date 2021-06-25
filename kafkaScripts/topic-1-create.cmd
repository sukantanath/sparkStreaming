::kafka-topics.bat --create --zookeeper localhost:2182 --topic notifications --partitions 1 --replication-factor 1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic invoices --partitions 1 --replication-factor 1 --config min.insync.replicas=1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic invoice-items --partitions 1 --replication-factor 1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic sensor --partitions 1 --replication-factor 1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic customer-rewards --partitions 1 --replication-factor 1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic trades --partitions 1 --replication-factor 1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic logins --partitions 1 --replication-factor 1
::kafka-topics.bat --create --zookeeper localhost:2182 --topic impressions --partitions 1 --replication-factor 1
kafka-topics.bat --create --zookeeper localhost:2182 --topic clicks --partitions 1 --replication-factor 1
