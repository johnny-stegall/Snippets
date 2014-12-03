DECLARE @Numbers TABLE(Number BIGINT)

DECLARE @Index SMALLINT,
  @Minimum BIGINT,
  @Maximum BIGINT

SET @Index = 0
SET @Minimum = 25
SET @Maximum = 100

WHILE (@Index < 1000)
BEGIN
  INSERT @Numbers
  SELECT @Minimum + ABS(CHECKSUM(NEWID())) % (@Maximum - (@Minimum - 1))

  SET @Index = @Index + 1
END

SELECT DISTINCT *
FROM @Numbers
ORDER BY Number
