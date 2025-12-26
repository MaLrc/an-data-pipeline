{{
	config(
		unique_key=['uid', 'acteurRef']
	)
}}

WITH cosignataires_arr AS (
  SELECT
    json_extract_string(json, '$.amendement.uid') AS uid,
    json_extract_string(json, '$.amendement.signataires.cosignataires.acteurRef[*]') AS acteurRef,
    date_T_insert
  FROM {{ref('stg_amendements')}}
  WHERE json_type(json, '$.amendement.signataires.cosignataires.acteurRef') == 'ARRAY' AND acteurRef IS NOT NULL
), cosignataires_str AS (
  SELECT
    json_extract_string(json, '$.amendement.uid') AS uid,
    json_extract_string(json, '$.amendement.signataires.cosignataires.acteurRef') AS acteurRef,
    date_T_insert
  FROM {{ref('stg_amendements')}}
  WHERE json_type(json, '$.amendement.signataires.cosignataires.acteurRef') <> 'ARRAY' AND acteurRef IS NOT NULL
), cosignataires AS (
	SELECT 
		uid, 
		unnest(acteurRef) as acteurRef,
		date_T_insert
	FROM cosignataires_arr
	UNION ALL
	SELECT 
		uid, 
		acteurRef,
		date_T_insert
	FROM cosignataires_str
)
SELECT * 
FROM cosignataires

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON cosignataires.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}