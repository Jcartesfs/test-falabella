WITH TABLE_PREGUNTAS AS (
SELECT OwnerUserId as UserId,
       SPLIT(CreationDate, '-')[OFFSET(0)] as Year,
       AVG(cast(score as float64)) as scoring_avg_preguntas,
       
       FROM `test-falabella-276616.dataset_falabella.Answers`
       WHERE REGEXP_CONTAINS(score,'[0-9]+') 
       group by UserId, Year
), TABLE_RESPUESTAS AS (
SELECT OwnerUserId as UserId,
       SPLIT(CreationDate, '-')[OFFSET(0)] as Year,
       AVG(cast(score as float64)) as scoring_avg_respuestas
       FROM `test-falabella-276616.dataset_falabella.Questions`
       WHERE REGEXP_CONTAINS(score,'[0-9]+')
       group by UserId, Year
)SELECT
      COALESCE(P.USERID,R.USERID) USERID,
      COALESCE(P.YEAR,R.YEAR) YEAR,
      COALESCE(P.scoring_avg_preguntas, NULL) scoring_avg_preguntas, # Se puede agregar un flag para identificar casos que no tengan preguntas
      COALESCE(R.scoring_avg_respuestas, NULL) scoring_avg_respuestas # Se puede agregar un flag para identificar casos que no tengan respuestas

      FROM TABLE_RESPUESTAS R 
      FULL OUTER JOIN TABLE_PREGUNTAS P 
                   ON P.USERID = R.USERID 
                  AND P.YEAR = R.YEAR 
     