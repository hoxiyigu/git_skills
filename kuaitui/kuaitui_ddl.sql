-- ����(hiv on es��)
drop table if exists lmmbase.hive_on_hbase_kuaitui_datasync_mid;
create external table lmmbase.hive_on_hbase_kuaitui_datasync_mid(
  rowkey string comment 'rowkey',
  user_id bigint comment '�û�id',
  user_name string comment '�û���',
  created_date timestamp comment 'ע��ʱ��',
  group_id string comment 'ע������',
  gender string comment '�Ա� F--Ů, M--��',
  age int comment '����',
  birthday timestamp comment '����',
  member_grade int comment '��Ա�ȼ�',
  phone_home_city string comment '�ֻ�������',
  city_id string comment '��ס��'
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,b:user_id,b:user_name,b:created_date, b:group_id,b:gender,b:age,b:birthday,b:member_grade,b:phone_home_city,b:city_id")
TBLPROPERTIES ("hbase.table.name" = "user_label");