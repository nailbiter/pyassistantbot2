select uuid, 1 as x, udf__md5(uuid) as hash
from tasks
order by scheduled_date desc
