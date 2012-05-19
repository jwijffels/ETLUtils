ETLUtils
=========

ETLUtils provides utility functions to execute standard ETL operations (using package ff) on large data.
Currently the following functions might be useful to you if you have some large dataset in SQL and want to import it immediately in an ffdf object

  - read.dbi.ffdf (reading of SQL data through DBI)
  - read.odbc.ffdf (reading of SQL data through RODBC)

An example can be found at http://www.bnosac.be/blog/19-get-your-large-sql-data-in-ff-swiftly

Other utilities
-----------

Other functions include matchmerge, recoder, naLOCFPlusone and renameColumns.

CRAN
-----------
This is the development version of the package which is available at CRAN.

* [ETLUtils @ CRAN]


  [ETLUtils @ CRAN]: http://cran.r-project.org/web/packages/ETLUtils/index.html
