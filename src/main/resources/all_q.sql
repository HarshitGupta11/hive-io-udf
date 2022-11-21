create external table csv_test(
startRow string,
rowid BIGINT,
length bigint,
dataCrc bigint,
dataRow string,
rowid2 bigint,
rowCrc bigint,
endRow bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION "abfs://harshitcontainer@harshitazurestorage.dfs.core.windows.net/tmp/non-quote/";

analyze table csv_test compute statistics for  columns;