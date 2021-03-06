#' @title Clean a table downloaded from TEMPO Online database
#' 
#' @description tempo_clean cleans a table downloaded from 
#' TEMPO Online database
#' 
#' @param matrix - the R dataframe object to be cleaned, representing 
#' the table/matrix downloaded from TEMPO Online database 
#' 
#' @param matrix_code - string containing the code for 
#' the table/matrix in TEMPO Online database. See more on
#' how to obtain a matrix code from \code{\link{tempo_codes}}
#'
#' @return Returns a R dataframe object. 

#' @details This function removes redundant columns or redundant information from columns
#' 
#' @examples tempo_clean(SOM101D, "SOM101D")
#' 
#' @export




tempo_clean <- function(matrix, matrix_code){
  if(is.null(matrix) | is.null(matrix_code) | !is.data.frame(matrix) | !is.character(matrix_code)){
    return(NULL)
  }
  
  column_names <- names(matrix)
  
  pos_ani = 0 #pozitia coloanei "ani", default 0 (undefined)
  pos_um = 0 #pozitia coloanei "unitate masura", default 0 (undefined)
  
  if(length(grep("([aA]ni|[yY]ears)", column_names)) > 0) {
    pos_ani <- grep("([aA]ni|[yY]ears)", column_names)[1]
  
    for(i in 1:nrow(matrix)) {
      matrix[i,pos_ani] <- gsub("([aA]nul|[yY]ear) *", "", matrix[i,pos_ani])
    }

    matrix[[pos_ani]] <- as.integer(matrix[[pos_ani]])
  }
  
  
  if(length(grep("(UM:|MU:)", column_names)) > 0){
    pos_um <- grep("(UM:|MU:)", column_names)[1]
    um <- gsub("(UM:|MU:) *", "", column_names[pos_um])
    
    matrix <- matrix[,-c(pos_um)]
    names(matrix)[names(matrix) == "Valoare" | names(matrix) == "Value"] <- paste0(matrix_code, "/", um)
  }
  
# Here the function overwrites the object in the Global Environment
# which has the same name as the name given as parameter, i.e. matrix_code
  assign(matrix_code, matrix, envir = .GlobalEnv)
}

