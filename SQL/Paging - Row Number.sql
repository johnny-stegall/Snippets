USE Customers
GO

IF (OBJECT_ID('{SP Name}') IS NOT NULL)
  DROP PROCEDURE dbo.{SP Name}
GO

/************************************************************
* Retrieves limited information about one or more customers.
************************************************************/
CREATE PROCEDURE dbo.{SP Name}
  @StartIndex INT,
  @PageSize INT

AS
BEGIN
	-- Prevents extra result sets from interfering with SELECT statements
	SET NOCOUNT ON;

  -- Add any filtering to the query below
  SELECT *,
    ROW_NUMBER() OVER(ORDER BY CompanyName1, LastName) AS RowId
  INTO #{Temp Table Name}

  IF (@PageSize < 1)
    SELECT * FROM #{Temp Table Name}
  ELSE
    SELECT * FROM #{Temp Table Name} WHERE RowId BETWEEN @StartIndex AND (@StartIndex + @PageSize)

  DECLARE @TotalRecords INT
  SELECT @TotalRecords = COUNT(*) FROM #{Temp Table Name}
  RETURN @TotalRecords

END
GO
