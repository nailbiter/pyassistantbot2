with t_tags as (
     select uuid
     	    , max(iif(tag='gstasks' or tag='pyas3',1,0)) is_recreational
     from tags
     group by uuid
)
select uuid,
iif(date(scheduled_date)=date('{{now}}'),'is_today','') as class
 , scheduled_date
 , iif(due is not null,'has_due','') as class_has_due
 , iif(status='ENGAGE','is_engage','') as class_is_engage
 , iif(status='PENDING','is_pending','') as class_is_pending
 , iif(date(due)<=date('{{now}}'),'is_past_due','') as class_is_past_due
 , datetime('{{now}}') as now
 , iif(t_tags.is_recreational, 'is_recreational', '') class_is_recreational
from tasks left join t_tags using (uuid)
