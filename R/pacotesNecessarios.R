#Pacotes utilizados no projeto R (ETL - GLPI)
pacotes <- c(
               "odbc"
              ,"DBI"
              ,"dplyr"
              ,"tidyverse"
              ,"dbplyr"
              ,"RMySQL"
              ,"RODBC"
              ,"rmarkdown"
            )
if(sum(as.numeric(!pacotes %in% installed.packages())) != 0){
  instalador <- pacotes[!pacotes %in% installed.packages()]
    for(i in 1:length(instalador)) {
      install.packages(instalador, dependencies = T)
      break()
    }
  msg <- "A instalação dos pacotes foi realizada com sucesso"
} else {
  msg <- "Todos os pacotes necessários já estão instalados"
}

print(msg)

