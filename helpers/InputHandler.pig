--- Input: e.g. in_date = 2013-05-29
%declare YEAR `echo $in_date | awk -F'-' '{print $1}'`
%declare MONTH `echo $in_date | awk -F'-' '{print $2}'`
%declare DAY `echo $in_date | awk -F'-' '{print $3}'`

SET mapred.min.split.size 3000000000;
SET mapred.max.split.size 3000000000;

/*SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo
*/

--- Get logs from root folder on HDFS ---
logs = LOAD 'prefilter-$in_date.out' USING PigStorage('\\u001') as (time_stamp:chararray, metric:chararray, service_flavour:chararray, hostname:chararray, status:chararray, vo:chararray, vofqan:chararray, profile:chararray);
--- Load new logs into Hive database ---
STORE logs INTO 'row_data' USING org.apache.hcatalog.pig.HCatStorer('year=$YEAR, month=$MONTH, day=$DAY');
