{{
	config(
		unique_key='uid'
	)
}}

WITH pays AS (
	SELECT
		json_extract_string(json, '$.pays.uid') as uid,
		json_extract_string(json, '$.pays.code_insee') as code_insee,
		json_extract_string(json, '$.pays.libelleInsee') as libelleInsee,
		json_extract_string(json, '$.pays.nomCourant') as nomCourant,
		json_extract_string(json, '$.pays.code_iso2A') as code_iso2A,
		json_extract_string(json, '$.pays.code_iso3A') as code_iso3A,
		json_extract_string(json, '$.pays.code_iso3N') as code_iso3N,
		json_extract_string(json, '$.pays.libelleIso') as libelleIso,
		json_extract_string(json, '$.pays.libelleANCourt') as libelleANCourt,
		json_extract_string(json, '$.pays.libelleANLong') as libelleANLong,
		json_extract_string(json, '$.pays.sourceJuridique') as sourceJuridique,
		json_extract_string(json, '$.pays.activ') as activ,
		date_T_insert
	FROM {{ref('stg_pays')}} source
)
SELECT * 
FROM pays

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON pays.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}