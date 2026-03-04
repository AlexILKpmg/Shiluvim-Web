CREATE DATABASE shiluvim_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
  
CREATE USER 'django_user'@'localhost'
IDENTIFIED BY 'StrongPassword123!';

-- 3) Give that user permissions only on this database
GRANT ALL PRIVILEGES ON shiluvim_db.* TO 'django_user'@'localhost';

-- 4) Apply permission changes immediately
FLUSH PRIVILEGES;


SHOW VARIABLES LIKE 'datadir';