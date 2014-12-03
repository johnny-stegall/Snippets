-- Make sure there's no master key before creating one
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE symmetric_key_id = 101)
  CREATE MASTER KEY ENCRYPTION BY 
  PASSWORD = '{Password}'

CREATE CERTIFICATE {SubjectOfEncryptedData}
WITH SUBJECT = 'Encrypted subject data goes here';

-- AES_256 can't be used in Windows 2000/XP, instead DES (weaker) must be used
CREATE SYMMETRIC KEY EncryptedData_Key_01
WITH ALGORITHM = AES_256
ENCRYPTION BY CERTIFICATE {SubjectOfEncryptedData};

/******************************************************************************
* Encrypt data
******************************************************************************/
USE {DatabaseName};

-- Create a column to store the encrypted data
ALTER TABLE {TableName}
ADD {EncryptedColumnName} varbinary(128); 

-- The encryption key must be opened once per session
OPEN SYMMETRIC KEY {EncryptedDataKey}
ENCRYPTION BY CERTIFICATE {SubjectOfEncryptedData};

-- Encrypt the clear text value
UPDATE TableName
SET {EncryptedColumnName} = ENCRYPTBYKEY(Key_GUID('{EncryptedDataKey}'), {ClearTextColumnName});

/******************************************************************************
* Decrypt data
******************************************************************************/
USE {DatabaseName};

-- The encryption key must be opened once per session
OPEN SYMMETRIC KEY {EncryptedDataKey}
DECRYPTION BY CERTIFICATE {SubjectOfEncryptedData};

-- Decrypt the encrypted value
SELECT {EncryptedColumnName},
  CAST(DECRYPTBYKEY({EncryptedColumnName}) AS NVARCHAR)
FROM {TableName};

/******************************************************************************
* Hashing data
******************************************************************************/
HASHBYTES('MD2 | MD4 | MD5 | SHA | SHA1', '{Text to hash}')