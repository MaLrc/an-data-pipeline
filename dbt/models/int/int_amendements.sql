{{
	config(
		unique_key='uid'
	)
}}

WITH amendements AS (
	SELECT
		json_extract_string(json, '$.amendement.uid') AS uid,
		json_extract_string(json, '$.amendement.chronotag') AS chronotag,
		json_extract_string(json, '$.amendement.examenRef') AS examenRef,
		json_extract_string(json, '$.amendement.amendementParentRef') AS amendementParentRef,
		json_extract_string(json, '$.amendement.signataires.auteur.typeAuteur') AS typeAuteur,
		json_extract_string(json, '$.amendement.signataires.auteur.acteurRef') AS refAuteur,
		json_extract_string(json, '$.amendement.signataires.auteur.groupePolitique') AS groupePolitiqueAuteur,
		json_extract_string(json, '$.amendement.cycleDeVie.dateDepot') AS dateDepot,
		json_extract_string(json, '$.amendement.cycleDeVie.datePublication') AS datePublication,
		json_extract_string(json, '$.amendement.cycleDeVie.soumisArticle40') AS soumisArticle40,
		json_extract_string(json, '$.amendement.cycleDeVie.etatDesTraitements.etat.code') AS codeEtat,
		json_extract_string(json, '$.amendement.cycleDeVie.etatDesTraitements.etat.libelle') AS libelleEtat,
		json_extract_string(json, '$.amendement.cycleDeVie.etatDesTraitements.sousEtat.code') AS codeSousEtat,
		json_extract_string(json, '$.amendement.cycleDeVie.etatDesTraitements.sousEtat.libelle') AS libelleSousEtat,
		json_extract_string(json, '$.amendement.cycleDeVie.etatDesTraitements.sousEtat.libelle') AS libelleSousEtat,
		json_extract_string(json, '$.amendement.cycleDeVie.dateSort') AS dateSort,
		json_extract_string(json, '$.amendement.cycleDeVie.sort') AS sort,
		split_part(replace(filename, '.json', ''), '/', -1) AS id_amendement,
		split_part(replace(filename, '.json', ''), '/', -2) AS id_texte,
		split_part(replace(filename, '.json', ''), '/', -3) AS id_dossier_legislatif,
		id_texte[:4] AS nature_texte,
		id_texte[5:6] AS source_texte,
		regexp_extract(id_texte, '(BTS|BTC|BTG|B)(\d*)', 1) AS prefix_bibard,
		regexp_extract(id_texte, '(BTS|BTC|BTG|B)(\d*)', 2) AS numero_bibard,
		regexp_extract(id_amendement, '(SEA|PO\d*)', 1) AS code_organe,
		regexp_extract(id_amendement, '(P)(\d*)(D)', 2) AS numero_partie,
		regexp_extract(id_amendement, '(D)(\d*)(N)', 2) AS numero_deliberation,
		regexp_extract(id_amendement, '(D)(\d*)(N)(\d*)', 4) AS numero_court,
		date_T_insert
	FROM {{ref('stg_amendements')}}
)
SELECT * 
FROM amendements

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON amendements.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}