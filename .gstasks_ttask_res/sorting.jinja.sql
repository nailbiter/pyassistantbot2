with t_tags as (
     select uuid, group_concat(tag) tagc
     from (
          select uuid,tag
          from tags
          order by tag
     )
     group by uuid
)
select uuid, 1 as x, udf__md5(uuid) as hash, tagc, is_not_soon
from (
     select *,
     	    scheduled_date is null as sdin,
	    ifnull((julianday(date(scheduled_date))-julianday(date('{{now}}')) > 2),0) as is_not_soon
     from tasks
) left join t_tags using (uuid)
order by is_not_soon, sdin, scheduled_date asc, tagc desc, _last_modification_date desc 
--limit 200 --debug
