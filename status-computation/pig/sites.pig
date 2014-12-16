--FOR MONGODB USAGE ANS SUPPORT
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-core.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-pig.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-java-driver-2.11.4.jar

SITES = load '$site_date' using PigStorage('\u0001') as
                        (hostname:chararray,service:chararray,p:chararray,m:chararray,s:chararray,site:chararray
                         ,roc:chararray,i:chararray,c:chararray,ss:chararray);

SITES_TRIM = FOREACH SITES GENERATE hostname as h,service as srv,site as st,roc as roc;

STORE SITES_TRIM INTO 'mongodb://$mongo_host:$mongo_port/AR.info_sites'     USING com.mongodb.hadoop.pig.MongoInsertStorage();
