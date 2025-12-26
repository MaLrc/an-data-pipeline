{{
	config(
		unique_key='uid'
	)
}}

WITH reunions AS (
	SELECT
		json_extract_string(json, '$.reunion.uid') AS uid,
		json_extract_string(json, '$.reunion.timeStampDebut') AS timeStampDebut,
		json_extract_string(json, '$.reunion.lieu.lieuRef') AS lieuRef,
		json_extract_string(json, '$.reunion.lieu.libelleLong') AS lieuLibelle,
		json_extract_string(json, '$.reunion.cycleDeVie.etat') AS cycleDeVieEtat,
		json_extract_string(json, '$.reunion.demandeur') AS demandeur,
		json_extract_string(json, '$.reunion.organeReuniRef') AS organeReuniRef,
		json_extract_string(json, '$.reunion.visioConference') AS visioConference,
		json_extract_string(json, '$.reunion.sessionRef') AS sessionRef,
		json_extract_string(json, '$.reunion.ouverturePresse') AS ouverturePresse,
		json_extract_string(json, '$.reunion.captationVideo') AS captationVideo,
		json_extract_string(json, '$.reunion.ODJ.convocationODJ.item') AS convocationODJ,
		json_extract_string(json, '$.reunion.ODJ.resumeODJ.item') AS resumeODJ,
		json_extract_string(json, '$.reunion.compteRenduRef') AS compteRenduRef,
		json_extract_string(json, '$.reunion.identifiants.numSeanceJO') AS numSeanceJO,
		json_extract_string(json, '$.reunion.identifiants.idJO') AS idJO,
		json_extract_string(json, '$.reunion.identifiants.quantieme') AS quantieme,
		json_extract_string(json, '$.reunion.identifiants.DataSeance') AS DataSeance,
		date_T_insert
	FROM {{ref('stg_reunions')}} source
)
SELECT * 
FROM reunions

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON reunions.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}