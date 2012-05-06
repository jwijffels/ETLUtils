###############################################################################
## Project: ETLUtils
## Content: Utilities that work for ff objects
##
## Author: jan
## Creation date: Mar 25, 2012, 10:06:53 PM
## File: ffutils.R
###############################################################################



#' Read data from a DBI connection into an ffdf.
#' 
#' Read data from a DBI connection into an ffdf. This can for example be used to import
#' huge datasets from Oracle, SQLite, RODBC, Informix, PostgreSQL or other SQL databases into R. \cr
#' 
#' Heavily borrowed from read.table.ffdf and read.odbc.ffdf on http://jthetzel.homeip.net/public/read.odbc.ffdf.r
#'
#' @param query the SQL query to execute on the DBI connection
#' @param dbConnect.args a list of arguments to pass to DBI's dbConnect (like drv, dbname, username, password). See the examples.
#' @param dbSendQuery.args a list containing database-specific parameters which will be passed to to pass to dbSendQuery. 
#' Defaults to an empty list.  
#' @param fetch.args a list containing optional database-specific parameters which will be passed to to pass to fetch. 
#' Defaults to an empty list.          
#' @param x NULL or an optional ffdf object to which the read records are appended. 
#' See documentation in read.table.ffdf for more details and the example below.
#' @param nrows Number of rows to read from the query resultset. Default value of -1 reads in all rows.
#' @param first.rows chunk size (rows) to read for first chunk from the query resultset
#' @param next.rows chunk size (rows) to read sequentially for subsequent chunks from the query resultset. Currently, this must be specified.
#' @param levels optional specification of factor levels. A list with as names the names the columns of the data.frame 
#' fetched in the first.rows, containing levels of the factors. 
#' @param appendLevels logical. A vector of permissions to expand levels for factor columns. See documentation in read.table.ffdf for more details.
#' @param asffdf_args further arguments passed to as.ffdf() (ignored if 'x' gives an ffdf object )
#' @param BATCHBYTES integer: bytes allowed for the size of the data.frame storing the result of reading one chunk. 
#' See documentation in read.table.ffdf for more details.
#' @param VERBOSE logical: TRUE to verbose timings for each processed chunk (default FALSE).
#' @param colClasses See documentation in read.table.ffdf
#' @param transFUN function applied to the data frame after each chunk is retreived by fetch
#' @param ... optional parameters passed on to transFUN
#' @return 
#' An ffdf object unless the query returns zero records in which case the function will return the data.frame
#' returned by fetch and possibly transFUN. 
#' @export
#' @seealso \code{\link[ff]{read.table.ffdf}}
#' @author Jan Wijffels
#' @examples
#' require(ff)
#' 
#' ##
#' ## Example query using data in sqlite
#' ##
#' require(RSQLite)
#' dbfile <- system.file("smalldb.sqlite3", package="ETLUtils")
#' drv <- dbDriver("SQLite")
#' query <- "select * from testdata limit 10000"
#' x <- read.dbi.ffdf(query = query, dbConnect.args = list(drv = drv, dbname = dbfile), 
#' first.rows = 100, next.rows = 1000, VERBOSE=TRUE)
#' class(x)
#' x[1:10, ]
#' 
#' ## show it is the same as getting the data directly using RSQLite apart from characters which are factors in ffdf objects
#' directly <- dbGetQuery(dbConnect(drv = drv, dbname = dbfile), query)
#' directly <- as.data.frame(as.list(directly), stringsAsFactors=TRUE)
#' all.equal(x[,], directly)
#'
#' ## show how to use the transFUN argument to transform the data before saving into the ffdf, and shows the use of the levels argument
#' query <- "select * from testdata limit 10"
#' x <- read.dbi.ffdf(query = query, dbConnect.args = list(drv = drv, dbname = dbfile), 
#' first.rows = 100, next.rows = 1000, VERBOSE=TRUE, levels = list(a = rev(LETTERS)),
#' transFUN = function(x, subtractdays){
#' 	x$b <- as.Date(x$b)
#' 	x$b.subtractdaysago <- x$b - subtractdays
#' 	x
#' }, subtractdays=7)
#' class(x)
#' x[1:10, ]
#' ## remark that the levels of column a are reversed due to specifying the levels argument correctly
#' levels(x$a)
#' 
#' ## show how to append data to an existing ffdf object 
#' transformexample <- function(x, subtractdays){
#' 	x$b <- as.Date(x$b)
#' 	x$b.subtractdaysago <- x$b - subtractdays
#' 	x
#' }
#' dim(x)
#' x[,]
#' combined <- read.dbi.ffdf(query = query, dbConnect.args = list(drv = drv, dbname = dbfile), 
#' first.rows = 100, next.rows = 1000, x = x, VERBOSE=TRUE, transFUN = transformexample, subtractdays=1000)
#' dim(combined)
#' combined[,]
#' 
#' ##
#' ## Example query using ROracle. Do try this at home with some larger data :)
#' ##
#' \dontrun{
#' require(ROracle)
#' query <- "select OWNER, TABLE_NAME, TABLESPACE_NAME, NUM_ROWS, LAST_ANALYZED from all_all_tables" 
#' x <- read.dbi.ffdf(query=query,
#' dbConnect.args = list(drv = dbDriver("Oracle"), 
#' user = "YourUser", password = "YourPassword", dbname = "Mydatabase"),
#' first.rows = 100, next.rows = 50000, nrows = -1, VERBOSE=TRUE)
#' }
read.dbi.ffdf <- function(
		query = NULL,
		dbConnect.args = list(drv=NULL, dbname = NULL, username = "", password = ""), 
		dbSendQuery.args = list(), 
		fetch.args = list(), 		
		x = NULL, nrows = -1, 
		first.rows = NULL, next.rows = NULL, levels = NULL, appendLevels = TRUE, 
		asffdf_args = list(), BATCHBYTES = getOption("ffbatchbytes"), VERBOSE = FALSE, colClasses = NULL, 
		transFUN = NULL, ...){
	
	# NOTE that this colClass implementation works only because accidentally the last position of the oldClasses is needed
	# for c("ordered","factor") read.table does not want "ordered"
	# for c("POSIXt","POSIXct") "POSIXct" is in the last position only due to "historical error" (Gabor Grothendieck, r-help, 26.9.2009)	
	colClass <- function(x){
		UseMethod("colClass")
	}	
	colClass.default <- function(x){
		cl <- class(x)
		cl[length(cl)]
	}	
	colClass.ff <- function(x){
		if (length(x))
			x <- x[1]
		else
			x <- x[]
		NextMethod()
	}
	convertCharacterToFactor <- function(x){
		chartofactor <- sapply(x, class)
		chartofactor <- chartofactor[chartofactor == "character"]
		for(convertme in names(chartofactor)){
			x[[convertme]] <- factor(x[[convertme]]) 
		}
		x
	}
	cleanupConnection <- function(x){
		if("resultset" %in% names(x)){
			dbClearResult(res=x$resultset)
		}
		if("channel" %in% names(x)){
			dbDisconnect(x$channel)
		}
	}
	dbiinfo <- list()
	
	##
	## Connect to database
	##
	dbiinfo$channel <- do.call('dbConnect', dbConnect.args)
	on.exit(cleanupConnection(dbiinfo))
	
	append <- !is.null(x)
	if(append && !inherits(x, "ffdf")){
		stop("only ffdf objects can be used for appending (and skipping the first.row chunk)")
	}
	if(VERBOSE){
		read.time <- 0
		write.time <- 0
	}
	
	nrows <- as.integer(nrows)
	N <- 0L	
	if(!append){		
		if(VERBOSE){
			cat("read.dbi.ffdf ", N+1L, "..", sep="")
			read.start <- proc.time()[3]
		}		
		if(is.null(colClasses)){
			colClasses <- NA
		}		
		## Specify the size of the first number of rows to import
		if (is.null(first.rows)){
			if (is.null(next.rows)){
				first.rows <- 1e3L
			}else{
				first.rows <- as.integer(next.rows)
			}				
		}else{
			first.rows <- as.integer(first.rows)
		}		
		if (nrows>=0L && nrows<first.rows){
			first.rows <- nrows
		}		
		if (first.rows==1){
			stop("first.rows must not be 1")
		}
		##
		## Set up arguments for odbcQuery and odbcFetchRows functions
		##
		dbSendQuery.args$conn <- dbiinfo$channel
		dbSendQuery.args$statement <- query
		##
		## Launch the query
		##
		dbiinfo$resultset <- do.call('dbSendQuery', dbSendQuery.args)
				
		##
		## Read in the first chunk
		##
		fetch.args$res <- dbiinfo$resultset
		fetch.args$n <- first.rows			
		if(is.null(transFUN)){
			dat <- convertCharacterToFactor(do.call("fetch", fetch.args))
		}else{
			dat <- convertCharacterToFactor(transFUN(do.call("fetch", fetch.args), ...))
		}				
		n <- nrow(dat)
		N <- n
		## Recode levels if levels are specified
		if(!is.null(levels)){
			cnam <- colnames(dat)
			lnam <- names(levels)
			if(!is.list(levels) || is.null(lnam) || any(is.na(match(lnam,cnam)))){
				stop("levels must be a list with names matching column names of the first data.frame read")
			}				
			for (i in lnam){
				dat[[i]] <- recodeLevels(dat[[i]], levels[[i]])
			}
		}		
		if (VERBOSE){
			write.start <- proc.time()[3]
			read.time <- read.time + (write.start - read.start)
			cat(N, " (", n, ")  dbi-read=", round(write.start-read.start, 3), "sec", sep="")
		}
		##
		## If there are zero rows in the query, return a data.frame instead of an ffdf object
		##
		if(nrow(dat) == 0){
			if (VERBOSE){
				cat(" query returned 0 records\n", sep="")
				cat(" dbi-read=", round(read.time, 3), "sec  TOTAL=", round(read.time, 3), "sec\n", sep="")
			}				
			return(dat)
		}
		
		##
		## If there are non-zero rows in the query, convert to a filebased ffdf object
		##
		x <- do.call("as.ffdf", c(list(dat), asffdf_args))		
		##
		## Fix ordered factors
		##
		colClasses <- repnam(colClasses, colnames(x), default=NA)
		i.fix <- seq.int(length.out=ncol(dat))[!is.na(match(colClasses, "ordered"))]
		for (i in i.fix){
			virtual(x[[i]])$ramclass <- c("ordered","factor")
		}		
		if (VERBOSE){
			write.stop <- proc.time()[3]
			write.time <- write.time + (write.stop - write.start)
			cat(" ffdf-write=", round(write.stop-write.start, 3), "sec\n", sep="")
		}
	}
	##
	## Fetch the other rows
	##
	if (append || N==first.rows){
		##
		## Define the number of rows to fetch in each run depending on the BATCHBYTES maximum
		##
		k <- ncol(x)
		col.names <- colnames(x)
		colClasses <- sapply(seq.int(length.out=ncol(x)), function(i)colClass(x[[i]]))		
		if (is.null(next.rows)){
			recordsize <- sum(.rambytes[vmode(x)])
			next.rows <- BATCHBYTES %/% recordsize
			if (next.rows<1L){
				next.rows <- 1L
				warning("single record does not fit into BATCHBYTES")
			}
		}else{
			next.rows <- as.integer(next.rows)
		}		
		next.nrows <- next.rows
		
		appendLevels <- repnam(appendLevels, col.names, default=TRUE)
		if(any(appendLevels)){
			i.fac <- seq.int(length.out=k)
			if(append == TRUE){
				i.fac <- i.fac[appendLevels & sapply(i.fac, function(i) is.factor(x[[i]]))]
			}else{
				i.fac <- i.fac[appendLevels & sapply(i.fac, function(i) is.factor(dat[[i]]))]	
			}
			
		}	
		if(append == TRUE){
			## Set up arguments for odbcQuery and odbcFetchRows functions & Launch the query
			dbSendQuery.args$conn <- dbiinfo$channel
			dbSendQuery.args$statement <- query			
			dbiinfo$resultset <- do.call('dbSendQuery', dbSendQuery.args)
		}
		##
		## Loop - fetching the subsequent data in batches
		##
		while(TRUE){
			if (nrows>=0L && N+next.rows > nrows){
				next.nrows <- nrows - N
			}				
			if (next.nrows<1L){
				break
			}			
			if (VERBOSE){
				cat("read.dbi.ffdf ", N+1L,"..", sep="")
				read.start <- proc.time()[3]
			}
			##
			## read in subsequent chunk
			##
			fetch.args$res <- dbiinfo$resultset
			fetch.args$n <- next.nrows
			if (is.null(transFUN)){
				dat <- convertCharacterToFactor(do.call("fetch", fetch.args))
			}else{
				dat <- convertCharacterToFactor(transFUN(do.call("fetch", fetch.args), ...))
			}			
			n <- nrow(dat)
			N <- N + n
			if (VERBOSE){
				write.start <- proc.time()[3]
				read.time <- read.time + (write.start - read.start)
				cat(N, " (", n, ")  dbi-read=", round(write.start-read.start, 3), "sec", sep="")
			}
			## Nothing to import any more
			if (n<1L){
				if (VERBOSE)
					cat("\n")
				break
			}
			## Recode levels if levels are specified
			if(any(appendLevels))
				for (i in i.fac){
					lev <- unique(c(levels(x[[i]]),levels(dat[[i]])))  # we save a call to the more general appendLevels() here
					levels(x[[i]]) <- lev
					dat[[i]] <- recodeLevels(dat[[i]], lev)
				}
			##
			## Append to the ffdf object
			##
			nff <- nrow(x)
			nrow(x) <- nff + n
			i <- hi(nff+1L, nff+n)
			x[i,] <- dat
			
			if (VERBOSE){
				write.stop <- proc.time()[3]
				write.time <- write.time + (write.stop - write.start)
				cat(" ffdf-write=", round(write.stop-write.start, 3), "sec\n", sep="")
			}			
			if(n<next.nrows){				
				break
			}				
		}		
	}
	if (VERBOSE){
		cat(" dbi-read=", round(read.time, 3), "sec  ffdf-write=", round(write.time, 3), "sec  TOTAL=", round(read.time+write.time, 3), "sec\n", sep="")
	}	
	return(x)
}

