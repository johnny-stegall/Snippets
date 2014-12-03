USE {DatabaseName}
GO

IF (OBJECT_ID('splitString') IS NOT NULL)
  DROP FUNCTION dbo.splitString
GO

/******************************************************************************
* Splits a string using the specified delimiter and returns a table of the
* split values.
******************************************************************************/
CREATE FUNCTION dbo.splitString
(
  @Input NVARCHAR(4000),
  @Delimiter NCHAR(1) = NULL
)
RETURNS @SplitTable TABLE (SplitValue NVARCHAR(4000))

AS
BEGIN
  DECLARE @SplitValue NVARCHAR(4000),
    @Found INT

  IF (@Delimiter IS NULL)
    SET @Delimiter = ','

  -- Loop through each character of the input string
  WHILE (LEN(@Input) > 0)
  BEGIN
    SET @Found = CHARINDEX(@Delimiter, @Input)
    
    IF (@Found > 0)
      SET @SplitValue = SUBSTRING(@Input, 1, @Found - 1)
    ELSE
    BEGIN
      SET @SplitValue = @Input
      SET @Input = ''
    END

    -- If the value isn't empty, put it in the table
    IF (LEN(@SplitValue) > 0)
      INSERT @SplitTable
      VALUES (@SplitValue)

    -- Move forward in the input string and increment the position
    SET @Input = SUBSTRING(@Input, @Found + 1, LEN(@Input))  
  END  

  RETURN
END