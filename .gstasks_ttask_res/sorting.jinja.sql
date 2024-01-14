select uuid, 1 as x, udf__md5(uuid) as hash, tagc, is_not_soon
from (
     select *,
     	    scheduled_date is null as sdin,
	    ifnull((julianday(date(scheduled_date))-julianday(date('{{now}}')) > 2),0) as is_not_soon
     from tasks
) left join (
     with t as (
     select uuid,tag
     from tags
     order by tag
     )
     select uuid, group_concat(tag) tagc
     from t
     group by uuid
) using (uuid)
order by is_not_soon, sdin, scheduled_date asc, tagc desc, _last_modification_date desc 
--limit 200 --debug
