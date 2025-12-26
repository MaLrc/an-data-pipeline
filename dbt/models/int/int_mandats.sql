{{
	config(
		unique_key='uid'
	)
}}

WITH mandats AS (
	SELECT 
		json_extract_string(mandat.value, '$.uid') as uid,
		json_extract_string(mandat.value, '$.acteurRef') as acteurRef,
		json_extract_string(mandat.value, '$.legislature') as legislature,
		json_extract_string(mandat.value, '$.typeOrgane') as typeOrgane,
		json_extract_string(mandat.value, '$.dateDebut') as dateDebut,
		json_extract_string(mandat.value, '$.dateFin') as dateFin,
		json_extract_string(mandat.value, '$.preseance') as preseance,
		json_extract_string(mandat.value, '$.nominPrincipale') as nominPrincipale,
		json_extract_string(mandat.value, '$.infosQualite.codeQualite') as codeQualite,
		json_extract_string(mandat.value, '$.organes.organeRef') as organeRef,
		date_T_insert
	FROM {{ref('stg_acteurs')}} act, json_each(json->'acteur'->'mandats'->'mandat') mandat
)
SELECT * 
FROM mandats

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON mandats.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}