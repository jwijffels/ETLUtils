ETLUtils
=========

ETLUtils provides utility functions to execute standard ETL operations (using package ff) on large data.
Currently the following functions might be useful to you if you have some large dataset in SQL and want to import it immediately in an ffdf object

  - read.dbi.ffdf (reading of SQL data through DBI)
  - read.odbc.ffdf (reading of SQL data through RODBC)
  - read.jdbc.ffdf (reading of SQL data through RJDBC)

An example can be found at http://www.bnosac.be/blog/19-get-your-large-sql-data-in-ff-swiftly

For users who want to store data from an ffdf back in a database, the package also provides

  - write.dbi.ffdf (writing of ffdf data to a database table through DBI)
  - write.odbc.ffdf (writing of ffdf data to a database table through RODBC)
  - write.jdbc.ffdf (writing of ffdf data to a database table through RJDBC)

Other utilities
-----------

Other functions include factorise, matchmerge, recoder, naLOCFPlusone and renameColumns.

CRAN
-----------
This is the development version of the package which is available at CRAN.

To install the latest version from github
```
devtools::install_github("jwijffels/ETLUtils")
```

* [ETLUtils @ CRAN]


  [ETLUtils @ CRAN]: http://cran.r-project.org/web/packages/ETLUtils/index.html
