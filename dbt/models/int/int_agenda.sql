{{
	config(
		unique_key=['Date', 'Heure']
	)
}}

SELECT * 
FROM {{ref('stg_agenda')}}