set hive.execution.engine=tez;

-- 微信小程序第三方联合登录表(去重)
drop table if exists lmmtmp.user_cooperation_wecat_tmp;
create table if not exists lmmtmp.user_cooperation_wecat_tmp stored as orc as
select t.* from (select *, (row_number() over (partition by id order by update_time desc)) rownum from lmmtmp.user_cooperation_wecat) t where rownum = 1;

-- 昨日登录用户id, 最后一次登录时间, 最后一次登录IP
drop table if exists lmmtmp.user_cooperation_wecat_last_tmp;
create table if not exists lmmtmp.user_cooperation_wecat_last_tmp stored as orc as
select t6.user_id, t6.created_date, t6.ip
  from (select user_id,
               created_date,
               ip,
               (row_number()
                over(partition by user_id order by update_time desc)) rownum
          from lmmtmp.user_action_collection
         where substr(created_date, 1, 10) = date_sub(current_date, 1)
           and login_type = 'WXAPP'
           and login_channel = 'WXAPP_LVMM_COMMON_NEW') t6
 where rownum = 1;

-- 快推数据临时表
drop table if exists lmmtmp.kuaitui_datasync_mid_tmp;
create table if not exists lmmtmp.kuaitui_datasync_mid_tmp stored as orc as
select t2.open_id,
       t3.user_name,
       1 is_register,
       t3.created_date register_time,
       if(t3.group_id = 'WXAPP_LVMM_COMMON_NEW', '微信小程序', null) register_channel,
       case t3.gender
         when 'M' then
          0
         when 'F' then
          1
         else
          2
       end gender,
       t3.age,
       t3.birthday,
       t3.member_grade,
       t4.city_name phone_city,
       t5.city_name resident_city,
       1 is_focus_wechat,
       t6.created_date last_login_time,
       t6.ip last_login_ip
  from (select distinct user_id 
          from (select user_id -- 1.1 昨天登录过微信小程序的用户
                  from lmmtmp.user_cooperation_wecat_last_tmp
                union all
                select user_id -- 1.2 昨天新注册微信小程序用户
                  from lmmtmp.user_user_day
                 where datestr = date_sub(current_date, 1)
                   and substr(created_date, 1, 10) =
                       date_sub(current_date, 1)
                   and group_id = 'WXAPP_LVMM_COMMON_NEW') t) t1 -- 1 昨天登录过微信小程序的用户和新注册微信小程序用户
  join (select open_id, max(user_id) user_id 
          from lmmtmp.user_cooperation_wecat_tmp
         group by open_id) t2
    on t1.user_id = t2.user_id -- 2 昨天登录过和新注册微信小程序用户
  join lmmbase.hive_on_hbase_kuaitui_datasync_mid t3
    on t1.user_id = t3.user_id -- 3 关联hive on hbase表(user_label), 获取会员其他信息
  left join lmmtmp.com_city t4
    on t3.phone_home_city = t4.city_id  -- 4 关联com_city表, 获取city_id对应的city_name
  left join lmmtmp.com_city t5
    on t3.city_id = t5.city_id -- 5 关联com_city表, 获取city_id对应的city_name
  left join user_cooperation_wecat_last_tmp t6
    on t1.user_id = t6.user_id;  -- 6 获取最后登录日期和ip
