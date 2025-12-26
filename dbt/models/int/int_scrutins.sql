{{
	config(
		unique_key='uid'
	)
}}

WITH scrutins AS (
	SELECT
		date_T_insert,
		json_extract_string(json, '$.scrutin.uid') AS uid,
		json_extract_string(json, '$.scrutin.numero') AS numero,
		json_extract_string(json, '$.scrutin.organeRef') AS organeRef,
		json_extract_string(json, '$.scrutin.legislature') AS legislature,
		json_extract_string(json, '$.scrutin.dateScrutin') AS date_scrutin,
		json_extract_string(json, '$.scrutin.titre') AS titre,
		json_extract_string(json, '$.scrutin.sort.code') AS resultat_code,
		json_extract_string(json, '$.scrutin.sort.libelle') AS resultat_libelle,
		json_extract_string(json, '$.scrutin.syntheseVote.nombreVotants') AS nb_votants,
		json_extract_string(json, '$.scrutin.syntheseVote.suffragesExprimes') AS suffrages_exprimes,
		json_extract_string(json, '$.scrutin.syntheseVote.nbrSuffragesRequis') AS seuil,
		json_extract_string(json, '$.scrutin.lieuVote') AS lieu_vote,
		date_T_insert
	FROM {{ref('stg_scrutins')}}
)
SELECT * 
FROM scrutins

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON scrutins.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}