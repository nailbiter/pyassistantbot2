select uuid, 1 as x, udf__md5(uuid) as hash, tagc
from (
     select *, scheduled_date is null as sdin
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
order by sdin, scheduled_date asc, tagc desc, _last_modification_date desc 

