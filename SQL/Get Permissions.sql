SET NOCOUNT OFF

DECLARE @Permissions TABLE
 ({Database Name} SYSNAME,
  [User/Role] SYSNAME,
  AccountType NVARCHAR(60),
  ActionType NVARCHAR(128),
  Permission NVARCHAR(60),
  [Object] SYSNAME NULL,
  ObjectType NVARCHAR(60))

-- Create the table variable to hold all databases
DECLARE @Databases TABLE ({Database Name} SYSNAME)

-- Insert the name of every database into the table variable
-- NOTE: Filter databases in the WHERE clause below
DECLARE @Next SYSNAME
INSERT INTO @Databases
SELECT [name]
FROM sys.databases
WHERE [name] NOT IN ('model')
ORDER BY [name]

SELECT TOP 1 @Next = {Database Name} FROM @Databases

WHILE (@@rowcount <> 0)
BEGIN
  INSERT INTO @Permissions
  EXEC ('USE [' + @Next + ']
    DECLARE @objects TABLE (obj_id INT, obj_type CHAR(2))

    INSERT @objects
    SELECT id, xtype
    FROM master.sys.sysobjects

    INSERT @objects
    SELECT object_id,
      type
    FROM sys.objects

    SELECT ''' + @Next + ''',
      ''User or Role Name'' = a.name,
      ''Account Type'' = a.type_desc,
      ''Type of Permission'' = d.permission_name,
      ''State of Permission'' = d.state_desc,
      ''Object Name'' = OBJECT_SCHEMA_NAME(d.major_id) + ''.'' + object_name(d.major_id),
      ''Object Type'' = CASE e.obj_type
        WHEN ''AF'' THEN ''Aggregate function (CLR)''
        WHEN ''C'' THEN ''CHECK constraint''
        WHEN ''D'' THEN ''DEFAULT (constraint or stand-alone)''
        WHEN ''F'' THEN ''FOREIGN KEY constraint''
        WHEN ''PK'' THEN ''PRIMARY KEY constraint''
        WHEN ''P'' THEN ''SQL stored procedure''
        WHEN ''PC'' THEN ''Assembly (CLR) stored procedure''
        WHEN ''FN'' THEN ''SQL scalar function''
        WHEN ''FS'' THEN ''Assembly (CLR) scalar function''
        WHEN ''FT'' THEN ''Assembly (CLR) table-valued function''
        WHEN ''R'' THEN ''Rule (old-style, stand-alone)''
        WHEN ''RF'' THEN ''Replication-filter-procedure''
        WHEN ''S'' THEN ''System base table''
        WHEN ''SN'' THEN ''Synonym''
        WHEN ''SQ'' THEN ''Service queue''
        WHEN ''TA'' THEN ''Assembly (CLR) DML trigger''
        WHEN ''TR'' THEN ''SQL DML trigger''
        WHEN ''IF'' THEN ''SQL inline table-valued function''
        WHEN ''TF'' THEN ''SQL table-valued-function''
        WHEN ''U'' THEN ''Table (user-defined)''
        WHEN ''UQ'' THEN ''UNIQUE constraint''
        WHEN ''V'' THEN ''View''
        WHEN ''X'' THEN ''Extended stored procedure''
        WHEN ''IT'' THEN ''Internal table''
      END
    FROM [' + @Next + '].sys.database_principals a 
      LEFT JOIN [' + @Next + '].sys.database_permissions d
        ON a.principal_id = d.grantee_principal_id
      LEFT JOIN @objects e
        ON d.major_id = e.obj_id
    ORDER BY a.name,
      d.class_desc')

  DELETE @Databases
  WHERE {DatabaseName} = @Next

  SELECT TOP 1 @Next = {DatabaseName}
  FROM @Databases
END

SET NOCOUNT OFF

SELECT * FROM @Permissions