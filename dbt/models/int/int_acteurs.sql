{{
	config(
		unique_key='uid'
	)
}}

WITH acteurs AS (
	SELECT 
		json_extract_string(json, '$.acteur.uid.#text') as uid,
		json_extract_string(json, '$.acteur.etatCivil.ident.civ') as civ,
		json_extract_string(json, '$.acteur.etatCivil.ident.prenom') as prenom,
		json_extract_string(json, '$.acteur.etatCivil.ident.nom') as nom,
		json_extract_string(json, '$.acteur.etatCivil.ident.trigramme') as trigramme,
		json_extract_string(json, '$.acteur.etatCivil.infoNaissance.dateNais') as dateNais,
		json_extract_string(json, '$.acteur.etatCivil.infoNaissance.villeNais') as villeNais,
		json_extract_string(json, '$.acteur.etatCivil.infoNaissance.depNais') as depNais,
		json_extract_string(json, '$.acteur.etatCivil.infoNaissance.paysNais') as paysNais,
		json_extract_string(json, '$.acteur.etatCivil.dateDeces') as dateDeces,
		json_extract_string(json, '$.acteur.profession.libelleCourant') as libelleCourant,
		json_extract_string(json, '$.acteur.profession.socProcINSEE.catSocPro') as catSocPro,
		json_extract_string(json, '$.acteur.profession.socProcINSEE.famSocPro') as famSocPro,
		json_extract_string(json, '$.acteur.uri_hatvp') as uri_hatvp,
		date_T_insert
	FROM {{ref('stg_acteurs')}}
)
SELECT *
FROM acteurs

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON acteurs.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}