-- 快推(hiv on es表)
drop table if exists lmmbase.hive_on_hbase_kuaitui_datasync_mid;
create external table lmmbase.hive_on_hbase_kuaitui_datasync_mid(
  rowkey string comment 'rowkey',
  user_id bigint comment '用户id',
  user_name string comment '用户名',
  created_date timestamp comment '注册时间',
  group_id string comment '注册渠道',
  gender string comment '性别 F--女, M--男',
  age int comment '年龄',
  birthday timestamp comment '生日',
  member_grade int comment '会员等级',
  phone_home_city string comment '手机归属地',
  city_id string comment '常住地'
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,b:user_id,b:user_name,b:created_date, b:group_id,b:gender,b:age,b:birthday,b:member_grade,b:phone_home_city,b:city_id")
TBLPROPERTIES ("hbase.table.name" = "user_label");