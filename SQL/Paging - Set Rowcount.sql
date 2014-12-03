USE Customers
GO

IF (OBJECT_ID('{SP Name}') IS NOT NULL)
  DROP PROCEDURE dbo.{SP Name}
GO

/**********************************************************************
* Retrieves a paged list of entities.
**********************************************************************/
CREATE PROCEDURE dbo.{SP Name}
  @StartIndex int,
  @PageSize int

AS
BEGIN
	-- Prevents extra result sets from interfering with SELECT statements
	SET NOCOUNT ON;

  -- Get the total number of records, place filters here in a WHERE clause
  DECLARE @TotalRecords int
  SELECT @TotalRecords = COUNT(*)
  FROM {Table Name}

  -- Set the page bounds
  DECLARE @SortColumn datetime
  IF (@StartIndex < 1)
    SET ROWCOUNT 1
  ELSE
    SET ROWCOUNT @StartIndex

  -- Get the first item as sorted by a unique key
  -- Note: Any filtering used above must be used here too
  SELECT @SortColumn = {Column Name to sort by}
  FROM {Table Name}
  ORDER BY {Column Name to sort by} DESC

  -- Set the row count to the size of the page and get the records
  IF (@PageSize < 1)
    SET ROWCOUNT @TotalRecords
  ELSE
    SET ROWCOUNT @PageSize

  -- Add additional filtering to the final query below
  SELECT *
  FROM {Table Name}
  WHERE {Column Name to sort by} <= @SortColumn
  ORDER BY {Column Name to sort by} DESC

  SET ROWCOUNT 0
  RETURN @TotalRecords

END
GO
