CREATE TABLE #Tables (TableName VARCHAR(100))

INSERT #Tables
VALUES ('{Table Name}')

DECLARE @Table VARCHAR(100)

DECLARE TableCursor CURSOR FAST_FORWARD FOR
  SELECT TableName FROM #Tables
OPEN TableCursor

FETCH NEXT FROM TableCursor INTO @Table

CREATE TABLE #RecordSizes ([Table] VARCHAR(100),
  RecordSize DECIMAL(8, 4))

WHILE (@@FETCH_STATUS = 0)
BEGIN
  SELECT C.name AS [Column],
    T.name AS [Type],
    T.max_length,
    T.scale,
    T.[precision]
  INTO #Columns
  FROM sys.columns C
    JOIN sys.types T
      ON C.user_type_id = T.user_type_id
  WHERE [object_id] = OBJECT_ID(@Table, 'U')

  -- Sum the length of each column, then add the NULL bitmap (used to manage
  -- nullability of columns) and then divide by 1024 to get kilobytes
  INSERT #RecordSizes
  SELECT @Table,
    ((SUM(max_length) + (2 + ((COUNT(*) + 7) / 8))) / 1024.0)
  FROM #Columns

  FETCH NEXT FROM TableCursor INTO @Table
  DROP TABLE #Columns
END

SELECT *
FROM #RecordSizes

CLOSE TableCursor
DEALLOCATE TableCursor

DROP TABLE #Tables
DROP TABLE #RecordSizes
