SELECT DISTINCT O.name
FROM sysobjects O
  JOIN syscomments C
    ON C.Id = O.Id
WHERE category = 0
  AND c.text like '%{tablename}%'
ORDER BY o.name