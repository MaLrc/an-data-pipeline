{{
	config(
		unique_key='uid'
	)
}}

WITH organes AS (
	SELECT 
		json_extract_string(json, '$.organe.uid') as uid,
		json_extract_string(json, '$.organe.codeType') as codeType,
		json_extract_string(json, '$.organe.libelle') as libelle,
		json_extract_string(json, '$.organe.libelleEdition') as libelleEdition,
		json_extract_string(json, '$.organe.libelleAbrege') as libelleAbrege,
		json_extract_string(json, '$.organe.libelleAbrev') as libelleAbrev,
		json_extract_string(json, '$.organe.viMoDe.dateDebut') as dateDebut,
		json_extract_string(json, '$.organe.viMoDe.dateAgrement') as dateAgrement,
		json_extract_string(json, '$.organe.viMoDe.dateFin') as dateFin,
		json_extract_string(json, '$.organe.organeParent') as organeParent,
		json_extract_string(json, '$.organe.chambre') as chambre,
		json_extract_string(json, '$.organe.regime') as regime,
		json_extract_string(json, '$.organe.legislature') as legislature,
		date_T_insert
	FROM {{ref('stg_organes')}}
)
SELECT * 
FROM organes

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON organes.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}