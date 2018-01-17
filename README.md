[![Build Status](https://travis-ci.org/jwijffels/ETLUtils.png?branch=master)](https://travis-ci.org/jwijffels/ETLUtils)
[![License](http://img.shields.io/badge/license-GPL%20%28%3E=%202%29-brightgreen.svg?style=flat)](http://www.gnu.org/licenses/gpl-2.0.html) [![Downloads](http://cranlogs.r-pkg.org/badges/ETLUtils?color=brightgreen)](http://cran.rstudio.com/package=ETLUtils)


ETLUtils
=========

ETLUtils provides utility functions to execute standard ETL operations (using package ff) on large data.
Currently the following functions might be useful to you if you have some large dataset in SQL and want to import it immediately in an ffdf object

  - read.dbi.ffdf (reading of SQL data through DBI)
  - read.odbc.ffdf (reading of SQL data through RODBC)
  - read.jdbc.ffdf (reading of SQL data through RJDBC)

An example can be found at http://www.bnosac.be/index.php/blog/5-get-your-large-sql-data-in-ff-swiftly and at http://www.bnosac.be/index.php/blog/6-readodbcffdf-a-readdbiffdf-for-fetching-large-corporate-sql-data

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

To install the latest version from github `remotes::install_github("jwijffels/ETLUtils")`

To get the lastest version from CRAN:

* [ETLUtils @ CRAN]


  [ETLUtils @ CRAN]: https://cran.r-project.org/package=ETLUtils
