-- Populate a table variable with object names
-- NOTE: Filter objects below (e.g. WHERE TYPE = 'P')
DECLARE @ObjectNames TABLE(SPName VARCHAR(100),
  Processed BIT)
INSERT @ObjectNames
SELECT name,
  0
FROM sys.objects
ORDER BY name

-- Get the name of the first object
DECLARE @Current VARCHAR(100)
SET @Current = (SELECT TOP 1 SPName FROM @ObjectNames WHERE Processed = 0)

-- Iterate the objects and get their text
DECLARE @ObjectText TABLE (SPText VARCHAR(MAX))
WHILE (@Current IS NOT NULL)
BEGIN
  INSERT @ObjectText
  EXEC sp_helptext @Current
  
  UPDATE @ObjectNames SET Processed = 1 WHERE SPName = @Current
  SET @Current = (SELECT TOP 1 SPName FROM @ObjectNames WHERE Processed = 0)
END

SELECT * FROM @ObjectText