SELECT DISTINCT
'acteurs' as source,
date_T_insert
FROM {{ref('int_acteurs')}}

UNION ALL 

SELECT DISTINCT
'agenda' as source,
date_T_insert
FROM {{ref('int_agenda')}}

UNION ALL 

SELECT DISTINCT
'amendements' as source,
date_T_insert
FROM {{ref('int_amendements')}}

UNION ALL 

SELECT DISTINCT
'organes' as source,
date_T_insert
FROM {{ref('int_organes')}}

UNION ALL 

SELECT DISTINCT
'pays' as source,
date_T_insert
FROM {{ref('int_pays')}}

UNION ALL 

SELECT DISTINCT
'reunions' as source,
date_T_insert
FROM {{ref('int_reunions')}}

UNION ALL 

SELECT DISTINCT
'scrutins' as source,
date_T_insert
FROM {{ref('int_scrutins')}}
