{{
	config(
		unique_key=['uid', 'acteurRef']
	)
}}

WITH grp AS (
	-- explode les groupes (1 par groupe)
SELECT
	json_extract_string(j.json, '$.scrutin.uid') AS uid,
	json_extract_string(g.value, '$.organeRef') AS groupe_organe,
	g.value AS groupe_json,
	j.date_T_insert
FROM {{ref('stg_scrutins')}} j,
json_each(json->'scrutin'->'ventilationVotes'->'organe'->'groupes'->'groupe') g
),

positions_array AS (
	-- 4 types de votes que DuckDB peut trouver : pours, contres, abstentions, nonVotants
	SELECT
		uid,
		date_T_insert,
		groupe_organe,
		'pour' AS type_vote,
		p.value AS vote_json
	FROM grp,
		json_each(groupe_json->'vote'->'decompteNominatif'->'pours'->'votant') p
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.pours.votant') == '[ARRAY]'

	UNION ALL
	SELECT
		uid,
		date_T_insert,
		groupe_organe,
		'contre' AS type_vote,
		c.value AS vote_json
	FROM grp,
		json_each(groupe_json->'vote'->'decompteNominatif'->'contres'->'votant') c
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.contres.votant') == '[ARRAY]'

	UNION ALL
	SELECT
		uid,
		date_T_insert,
		groupe_organe,
		'abstention' AS type_vote,
		a.value AS vote_json
	FROM grp,
		json_each(groupe_json->'vote'->'decompteNominatif'->'abstentions'->'votant') a
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.abstentions.votant') == '[ARRAY]'

	UNION ALL
	SELECT
		uid,
		date_T_insert,
		groupe_organe,
		'nonVotant' AS type_vote,
		n.value AS vote_json
	FROM grp,
			json_each(groupe_json->'vote'->'decompteNominatif'->'nonVotants'->'votant') n
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.nonVotants.votant') == '[ARRAY]'
),
positions_object AS (
	-- 4 types de votes que DuckDB peut trouver : pours, contres, abstentions, nonVotants
	SELECT
		uid,
		groupe_organe,
		'pour' AS type_vote,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.pours.votant.acteurRef') AS acteurRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.pours.votant.mandatRef') AS mandatRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.pours.votant.parDelegation') AS parDelegation,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.pours.votant.numPlace') AS numPlace,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.pours.votant.causePositionVote') AS cause,
		date_T_insert
	FROM grp
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.pours.votant') == '[OBJECT]'

	UNION ALL
	SELECT
		uid,
		groupe_organe,
		'contre' AS type_vote,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.contres.votant.acteurRef') AS acteurRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.contres.votant.mandatRef') AS mandatRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.contres.votant.parDelegation') AS parDelegation,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.contres.votant.numPlace') AS numPlace,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.contres.votant.causePositionVote') AS cause,
		date_T_insert
	FROM grp
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.contres.votant') == '[OBJECT]'

	UNION ALL
	SELECT
		uid,
		groupe_organe,
		'abstention' AS type_vote,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.abstentions.votant.acteurRef') AS acteurRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.abstentions.votant.mandatRef') AS mandatRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.abstentions.votant.parDelegation') AS parDelegation,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.abstentions.votant.numPlace') AS numPlace,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.abstentions.votant.causePositionVote') AS cause,
		date_T_insert
	FROM grp
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.abstentions.votant') == '[OBJECT]'

	UNION ALL
	SELECT
		uid,
		groupe_organe,
		'nonVotant' AS type_vote,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.nonVotants.votant.acteurRef') AS acteurRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.nonVotants.votant.mandatRef') AS mandatRef,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.nonVotants.votant.parDelegation') AS parDelegation,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.nonVotants.votant.numPlace') AS numPlace,
		json_extract_string(groupe_json, '$.vote.decompteNominatif.nonVotants.votant.causePositionVote') AS cause,
		date_T_insert
	FROM grp
	WHERE json_type(groupe_json, '$..vote.decompteNominatif.nonVotants.votant') == '[OBJECT]'
),

clean AS (
	-- sécurise les champs (souvent string ou null)
	SELECT
		uid,
		groupe_organe,
		type_vote,
		json_extract_string(vote_json, '$.acteurRef') AS acteurRef,
		json_extract_string(vote_json, '$.mandatRef') AS mandatRef,
		json_extract_string(vote_json, '$.parDelegation') AS parDelegation,
		json_extract_string(vote_json, '$.numPlace') AS numPlace,
		json_extract_string(vote_json, '$.causePositionVote') AS cause,
		date_T_insert
	FROM positions_array
	UNION ALL
	SELECT * 
	FROM positions_object
)

SELECT * FROM clean

{% if is_incremental() %}
	LEFT JOIN {{ this }} AS target ON clean.uid = target.uid
	WHERE target.uid IS NULL
{% endif %}
