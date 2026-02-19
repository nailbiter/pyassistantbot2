select uuid,
iif(date(scheduled_date)=date('{{now}}'),'is_today','') as class
 ,scheduled_date
 ,iif(due is not null,'has_due','') as class_has_due
 ,iif(status='ENGAGE','is_engage','') as class_is_engage
 ,iif(date(due)<=date('{{now}}'),'is_past_due','') as class_is_past_due
 ,datetime('{{now}}') as now
--status as class \n
from tasks
