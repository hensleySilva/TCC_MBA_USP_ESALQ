################################################################################
############################     DATA MINING   #################################
################################################################################

print("Iniciando programa de ETL")

##while(T) {

  ## Cria função para gravação de log de execução no diretório do projeto
  writeLog <- function(msg){
    if (!file.exists("log")){dir.create("log")}
    logFile <- paste(
        getwd()
        ,"/log/log_projetoR_"
        ,format(Sys.time(), format = "%Y%m%d")
        ,".txt"
        ,sep =  ""
      )
    logFile <- file(logFile, open = "a")
    cat(format(Sys.time(), format = "%Y-%m-%d %H:%M:%OS3"),";",msg,"\n",file = logFile)
    close(logFile)
  }
  
  ## mensagem padrão em caso de falha na execução do programa
  stopMsg = "Ocorreu um erro. Consulte o arquivo de log de execução para obter mais detalhes sobre o erro. Encerrando programa."
  
  startTime <- as.POSIXct(Sys.time(), tz = "UTC")
  
  writeLog("[Inicializando]")
  
  print("Carregando pacotes")
  
  writeLog("Carregando pacotes")
  tryCatch({
    library("DBI")
    library("odbc")
    library("RMySQL")
    library("RODBC")
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    writeLog("Verificando pacotes necessários")
    source("pacotesNecessarios.R")
    stop(stopMsg)
  }
  )
  
  writeLog("Carregando variaveis de ambiente")
  tryCatch({
    readRenviron(".Renviron")
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  #carregar váriaveis de ambiente para conexão com GLPI
  db_user_glpi <- Sys.getenv("DB_USER_GLPI")
  db_password_glpi <- Sys.getenv("DB_PASSWORD_GLPI")
  
  print("Conectando ao banco GLPI")
  
  writeLog("Conectando ao banco GLPI")
  tryCatch({
    conn <- dbConnect(
       drv = RMySQL::MySQL()
      ,dbname = 'glpi_prod'
      ,host = '10.110.255.118'
      ,username    = db_user_glpi
      ,password    = db_password_glpi
      ,port = 3306
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  ################################################################################
  ####################  EXTRAÇÃO DE DADOS DO GLPI ################################
  ################################################################################
  print("Executando consulta no banco GLPI (Origem)")
  
  writeLog("Executando consulta no banco GLPI (Origem)")
  tryCatch({
    sql <- dbSendQuery(
      conn
      ,"
        select
        	 t.id as id_chamado
        	,REPLACE(t.name,';',' ') as titulo_chamado
        	,t.date_creation as data_abertura_chamado
        	,t.date_mod as data_modificacao_chamado
        	,t.solvedate as data_solucao_chamado
        	,t.closedate as data_fechamento_chamado
        	,COALESCE(grupo_atribuido.setor,'(Não atribuído)') as setor_atribuido
        	,COALESCE(REPLACE(usuario_atribuido.nome_analista_atribuido,';',' '),'(Não atribuído)') as nome_analista_atribuido
        	,COALESCE(REPLACE(usuario_atribuido.login_analista_atribuido,';',' '),'(Não atribuído)') as login_analista_atribuido
        	,t.users_id_lastupdater as user_id_lastupdater
        	,u.name as user_login_lastupdater
        	,REPLACE(concat(u.firstname,' ',u.realname),';',' ') as user_name_lastupdater
        	,t.status as id_status_chamado				/*	1 - novo | 2 - processando (atribuído)	| 4 - pendente	| 5 - solucionado	| 6 - fechado */
        	,(case
        		when t.status = 1 then 'Novo'
        		when t.status = 2 then 'Processando'
        		when t.status = 4 then 'Pendente'
        		when t.status = 5 then 'Solucionado'
        		when t.status = 6 then 'Fechado'
        		else 'n/a'
        	end) as status_chamado
        	,t.type	as id_tipo_chamado				/*	1 - incidente | 2 - requisição */
        	,(case 
        		when t.type = 1 then 'Incidente'
        		when t.type = 2 then 'Requisição'
        		else 'n/a'
        	end) as tipo_chamado
        	,t.itilcategories_id as id_categoria
        	,REPLACE(ic.completename,';',' ') as categoria
        	,REPLACE(ic.name,';',' ') as subcategoria
        	#,t.content as descricao_chamado							/*		##	REMOVIDO POR INVIABILIDADE DE TRATAMENTO DO CAMPO	##	*/
        	,usuario_requerente.id as id_requerente
        	,usuario_requerente.name as login_requerente
        	,REPLACE(concat(usuario_requerente.firstname,' ',usuario_requerente.realname),';',' ') as nome_requerente
        	,grupo_requerente.setor  as setor_requerente
        	,solution.status as id_status_solucao
        	,solution.status_desc as status_solucao
        	#,solution.content as descricao_solucao			   /*		##	REMOVIDO POR INVIABILIDADE DE TRATAMENTO DO CAMPO	##	*/
        	,solution.date_creation as data_solucao
        	,solution.users_id as id_analista_solucao
        	,solution.users_login as login_analista_solucao
        	,REPLACE(solution.users_name,';',' ') as nome_analista_solucao
        	,solution.users_id_approval as id_avaliador_solucao
        	,solution.users_login_approval as login_avaliador_solucao
        	,REPLACE(solution.users_name_approval,';',' ') as nome_avaliador_solucao
        	,current_timestamp() as data_coleta_glpi
        from
        	glpi_tickets t
        	inner join glpi_itilcategories ic on ic.id = t.itilcategories_id
        	left join (
        		select
        			tu.tickets_id,u.id,u.name,u.firstname,u.realname
        		from
        			glpi_tickets_users tu
        			left join glpi_users u on u.id = tu.users_id
        		where
        			tu.type = 1		/*	type: 1 = requerente | 2 = atribuído | 3 = observador	*/
        	) as usuario_requerente on usuario_requerente.tickets_id = t.id
        	left join (
        		select
        			tickets_id,GROUP_CONCAT(g.name) as setor
        		from
        			glpi_groups_tickets gt
        			left join  glpi_groups g on g.id = gt.groups_id
        		where
        			gt.type = 1 /*	type: 1 = requerente | 2 = atribuido para | 3 = observador	*/
        		group by
        			tickets_id
        	) as grupo_requerente on grupo_requerente.tickets_id = t.id
        	left join (
        		select
        			tu.tickets_id,GROUP_CONCAT(u.name) as login_analista_Atribuido,GROUP_CONCAT(concat(u.firstname,' ',u.realname)) as nome_analista_atribuido
        		from
        			glpi_tickets_users tu
        			left join glpi_users u on u.id = tu.users_id
        		where
        			tu.type = 2		/*	type: 1 = requerente | 2 = atribuído | 3 = observador	*/
        		group by
        			tickets_id
        	) as usuario_atribuido on usuario_atribuido.tickets_id = t.id
        	left join (
        		select
        			tickets_id,GROUP_CONCAT(g.name) as setor
        		from
        			glpi_groups_tickets gt
        			left join  glpi_groups g on g.id = gt.groups_id
        		where
        			gt.type = 2 /*	type: 1 = requerente | 2 = atribuido para | 3 = observador	*/
        		group by
        			tickets_id
        	) as grupo_atribuido on grupo_atribuido.tickets_id = t.id
        	left join (
        		select
        			 s.items_id
        			,s.content
        			,s.date_creation
        			,s.date_approval
        			,s.users_id
        			,u.name as users_login
        			,concat(u.firstname,' ',u.realname) as users_name
        			,s.users_id_approval
        			,ua.name as users_login_approval
        			,concat(ua.firstname,' ',ua.realname) as users_name_approval
        			,s.status			/*		2 = solucionado (pendente aprovação) | status da solução: 3 = aprovado | 4 = reprovado	*/
        			,(case
        				when status = 2 then 'solucionado (aprovação pendente)'
        				when status = 3 then 'aprovado'
        				when status = 4 then 'reprovado'
        				else 'n/a'
        			end) as status_desc
        		from
        			glpi_itilsolutions s
        			inner join (
        				select
        					items_id,max(id) as id
        				from
        					glpi_itilsolutions s
        				group by
        					items_id
        			) as sq on sq.id = s.id
        			left join glpi_users u on u.id = s.users_id
        			left join glpi_users ua on ua.id = s.users_id_approval
        	) as solution on solution.items_id = t.id
        	left join glpi_users u on u.id = t.users_id_lastupdater
        where
        	t.date_mod >=  date_sub(curdate(),interval 7 day)
        limit 100000;
      "
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  writeLog("Gravando resultado da consulta")
  tryCatch({
    ## grava o resultado da query no data frame do R
    df_chamados_glpi <- dbFetch(sql, n = -1)
    
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  ## Limpa o resultado do dbSendQuery()
  dbClearResult(sql)
  
  print("Executando consulta no banco GLPI (Origem)")
  
  ## COLETA USUARIOS E GRUPOS DO GLPI
  writeLog("Executando consulta no banco GLPI (Origem)")
  tryCatch({
    sql <- dbSendQuery(
      conn
      ,"
        select
        	 u.id as id_usuario
        	,u.name as login_usuario
        	,upper(concat(u.firstname,' ', u.realname)) as nome_usuario
        	,(case when u.is_active = 1 then 'Ativo' else 'Inativo' end) as status_usuario
        	,g.id as id_grupo
        	,upper(g.name) as nome_grupo
        	,g.level level_grupo
        	,(case 
        		when g.level = 1 then 'Primário'
        		when g.level = 2 then 'Setor'
        		when g.level = 3 then 'Subsetor'
        	end) as tipo_grupo
        	,g.completename as nome_completo_grupo
        from
        	glpi_users u
        	left join glpi_groups_users gu on gu.users_id = u.id
        	left join glpi_groups g on g.id = gu.groups_id
  
    "
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  writeLog("Gravando resultado da consulta")
  tryCatch({
    ## grava o resultado da query no data frame do R
    df_usuarios_grupos_glpi <- dbFetch(sql, n = -1)
    
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  ## Limpa o resultado do dbSendQuery()
  dbClearResult(sql)
  
  print("Encerrando conexão com o banco GLPI")
  
  writeLog("Encerrando conexão com o banco GLPI")
  ## Disconectar do db GLPI
  dbDisconnect(conn)
  
  ################################################################################
  ##########  TRANSFORMAÇÃO E CARREGAMENTO DE DADOS ##############################
  ################################################################################
  
  #carregar váriaveis de ambiente para conexão com O banco do Atlas
  db_user_atlas <- Sys.getenv("DB_USER_ATLAS")
  db_password_atlas <- Sys.getenv("DB_PASSWORD_ATLAS")
  
  print("Conectando ao banco Atlas (Destino)")
  
  writeLog("Conectando ao banco Atlas (Destino)")
  tryCatch({
    ## string de conexão com o SQL SERVER utilizando o Driver ODBC
    conn <- DBI::dbConnect(
       odbc::odbc()
      ,Driver   = "SQL Server"
      ,Server   = "VSQL4"
      ,Database = "atlas"
      ,UID      = db_user_atlas
      ,PWD      = db_password_atlas
      ,Port     = 1433
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    stop(stopMsg)
  }
  )
  
  print("Preparando tabela de destino DBM_ETL_GLPI_CHAMADOS")
  
  writeLog("Preparando tabela de destino")
  tryCatch({
    sql <- dbSendQuery(
      conn
      ,"
        DROP TABLE IF EXISTS DBM_ETL_GLPI_CHAMADOS
        
        /* ################################## CRIA TABELA DE IMPORTACAO (ETL) ################################## */  
      
        IF OBJECT_ID('ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS') IS NULL BEGIN  
         CREATE TABLE ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS (  
            ID_CHAMADO INT  
           ,TITULO_CHAMADO VARCHAR(300)  
           ,DATA_ABERTURA_CHAMADO DATETIME  
           ,DATA_MODIFICACAO_CHAMADO DATETIME  
           ,DATA_SOLUCAO_CHAMADO DATETIME  
           ,DATA_FECHAMENTO_CHAMADO DATETIME  
           ,SETOR_ATRIBUIDO VARCHAR(300)  
           ,NOME_ANALISTA_ATRIBUIDO VARCHAR(500)  
           ,LOGIN_ANALISTA_ATRIBUIDO VARCHAR(200) 
           ,USER_ID_LASTUPDATER INT  
           ,USER_LOGIN_LASTUPDATER VARCHAR(100)    
           ,USER_NAME_LASTUPDATER  VARCHAR(300)  
           ,ID_STATUS_CHAMADO  INT  
           ,STATUS_CHAMADO  VARCHAR(50)  
           ,ID_TIPO_CHAMADO  INT  
           ,TIPO_CHAMADO  VARCHAR(50)  
           ,ID_CATEGORIA  INT  
           ,CATEGORIA  VARCHAR(500)  
           ,SUBCATEGORIA  VARCHAR(300)  
           ,ID_REQUERENTE  INT  
           ,LOGIN_REQUERENTE  VARCHAR(100)  
           ,NOME_REQUERENTE  VARCHAR(300)  
           ,SETOR_REQUERENTE  VARCHAR(500)  
           ,ID_STATUS_SOLUCAO  INT  
           ,STATUS_SOLUCAO  VARCHAR(50)  
           ,DATA_SOLUCAO  DATETIME  
           ,ID_ANALISTA_SOLUCAO  INT  
           ,LOGIN_ANALISTA_SOLUCAO  VARCHAR(100)  
           ,NOME_ANALISTA_SOLUCAO  VARCHAR(300)  
           ,ID_AVALIADOR_SOLUCAO  INT  
           ,LOGIN_AVALIADOR_SOLUCAO  VARCHAR(100)  
           ,NOME_AVALIADOR_SOLUCAO  VARCHAR(300)  
           ,DATA_COLETA_GLPI  DATETIME  
         )  
          
         CREATE CLUSTERED INDEX IX_ID_CHAMADO ON ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS (ID_CHAMADO)  
         CREATE NONCLUSTERED INDEX IX_DATA_MODIFICACAO_CHAMADO ON ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS (DATA_MODIFICACAO_CHAMADO)  
        END 
      "
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    dbClearResult(sql)
    stop(stopMsg)
  }
  )
  
  dbClearResult(sql)
  
  print("Importação de dados em DBM_ETL_GLPI_CHAMADOS")
  
  writeLog("Importação de dados")
  
  tryCatch({
    dbWriteTable(
       conn
      ,name = 'DBM_ETL_GLPI_CHAMADOS'
      ,value = df_chamados_glpi
      ,append = T
      
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    dbClearResult(sql)
    stop(stopMsg)
  }
  )
  
  ################################################################################
  print("Preparando tabela de destino DBM_GLPI_GRUPOS_USUARIOS")
  
  writeLog("Preparando tabela de destino (ATLAS.DBO.DBM_GLPI_GRUPOS_USUARIOS)")
  tryCatch({
    sql <- dbSendQuery(
      conn
      ,"
        DROP TABLE IF EXISTS ATLAS.DBO.DBM_GLPI_GRUPOS_USUARIOS
        
        IF OBJECT_ID('ATLAS.DBO.DBM_GLPI_GRUPOS_USUARIOS') IS NULL BEGIN
        	CREATE TABLE ATLAS.DBO.DBM_GLPI_GRUPOS_USUARIOS (
        		 ID_USUARIO INT
        		,LOGIN_USUARIO VARCHAR(30)
        		,NOME_USUARIO VARCHAR(100)
        		,STATUS_USUARIO VARCHAR(20)
        		,ID_GRUPO INT
        		,NOME_GRUPO VARCHAR(50)
        		,LEVEL_GRUPO INT
        		,TIPO_GRUPO VARCHAR(30)
        		,NOME_COMPLETO_GRUPO VARCHAR(500)
        	)
          CREATE CLUSTERED INDEX IX_LOGIN_USUARIO ON ATLAS.DBO.DBM_GLPI_GRUPOS_USUARIOS (LOGIN_USUARIO)
        END
      "
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    dbClearResult(sql)
    stop(stopMsg)
  }
  )
  
  dbClearResult(sql)
  
  print("Importação de dados (DBM_GLPI_GRUPOS_USUARIOS)")
  
  writeLog("Importação de dados em DBM_GLPI_GRUPOS_USUARIOS")
  
  tryCatch({
    dbWriteTable(
      conn
      ,name = 'DBM_GLPI_GRUPOS_USUARIOS'
      ,value = df_usuarios_grupos_glpi
      ,append = T
      
    )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    dbClearResult(sql)
    stop(stopMsg)
  }
  )
  ################################################################################
  print("Tratamento de dados")
  
  writeLog("Tratamento de dados")
  
  tryCatch({
  sql <- dbSendQuery(
     conn
    ,"
      
    /* #########################################################################*/  
    /* ####################   CRIA DATA WAREHOUSE (DW) #########################*/  
    
    IF OBJECT_ID('ATLAS.DBO.DBM_GLPI_CHAMADOS') IS NULL BEGIN  
     CREATE TABLE ATLAS.DBO.DBM_GLPI_CHAMADOS (  
        ID_CHAMADO INT 
       ,TITULO_CHAMADO VARCHAR(300)  
       ,DATA_ABERTURA_CHAMADO DATETIME  
       ,DATA_MODIFICACAO_CHAMADO DATETIME  
       ,DATA_SOLUCAO_CHAMADO DATETIME  
       ,DATA_FECHAMENTO_CHAMADO DATETIME  
       ,SETOR_ATRIBUIDO VARCHAR(300)  
       ,NOME_ANALISTA_ATRIBUIDO VARCHAR(500)  
       ,LOGIN_ANALISTA_ATRIBUIDO VARCHAR(300) 
       ,USER_ID_LASTUPDATER INT  
       ,USER_LOGIN_LASTUPDATER VARCHAR(100)    
       ,USER_NAME_LASTUPDATER  VARCHAR(300)  
       ,ID_STATUS_CHAMADO  INT  
       ,STATUS_CHAMADO  VARCHAR(50)  
       ,ID_TIPO_CHAMADO  INT  
       ,TIPO_CHAMADO  VARCHAR(50)  
       ,ID_CATEGORIA  INT  
       ,CATEGORIA  VARCHAR(500)  
       ,SUBCATEGORIA  VARCHAR(300)  
       ,ID_REQUERENTE  INT  
       ,LOGIN_REQUERENTE  VARCHAR(100)  
       ,NOME_REQUERENTE  VARCHAR(300)  
       ,SETOR_REQUERENTE  VARCHAR(500)  
       ,ID_STATUS_SOLUCAO  INT  
       ,STATUS_SOLUCAO  VARCHAR(50)  
       ,DATA_SOLUCAO  DATETIME  
       ,ID_ANALISTA_SOLUCAO  INT  
       ,LOGIN_ANALISTA_SOLUCAO  VARCHAR(100)  
       ,NOME_ANALISTA_SOLUCAO  VARCHAR(300)  
       ,ID_AVALIADOR_SOLUCAO  INT  
       ,LOGIN_AVALIADOR_SOLUCAO  VARCHAR(100)  
       ,NOME_AVALIADOR_SOLUCAO  VARCHAR(300)  
       ,DATA_COLETA_GLPI  DATETIME  
       ,DATA_INSERT DATETIME DEFAULT GETDATE()  
       ,DATA_UPDATE DATETIME  
       ,SETOR_REQUERENTE_TRATADO VARCHAR(100)  
     )  
     CREATE CLUSTERED INDEX IX_ID_CHAMADO ON ATLAS.DBO.DBM_GLPI_CHAMADOS (ID_CHAMADO)
     CREATE NONCLUSTERED INDEX IX_DATA_MODIFICACAO_CHAMADO ON ATLAS.DBO.DBM_GLPI_CHAMADOS (DATA_MODIFICACAO_CHAMADO)  
    END  
      
    /* ######################################################################## */  
    /* ##########	    ATUALIZA CHAMADOS EXISTENTES   ########################## */  
      
    UPDATE DGC SET   
        DGC.TITULO_CHAMADO = DEGC.TITULO_CHAMADO  
       ,DGC.DATA_MODIFICACAO_CHAMADO = DEGC.DATA_MODIFICACAO_CHAMADO  
       ,DGC.DATA_SOLUCAO_CHAMADO = DEGC.DATA_SOLUCAO_CHAMADO  
       ,DGC.DATA_FECHAMENTO_CHAMADO = DEGC.DATA_FECHAMENTO_CHAMADO  
       ,DGC.SETOR_ATRIBUIDO = DEGC.SETOR_ATRIBUIDO  
       ,DGC.NOME_ANALISTA_ATRIBUIDO = DEGC.NOME_ANALISTA_ATRIBUIDO  
       ,DGC.LOGIN_ANALISTA_ATRIBUIDO = DEGC.LOGIN_ANALISTA_ATRIBUIDO
       ,DGC.USER_ID_LASTUPDATER = DEGC.USER_ID_LASTUPDATER  
       ,DGC.USER_LOGIN_LASTUPDATER = DEGC.USER_LOGIN_LASTUPDATER  
       ,DGC.USER_NAME_LASTUPDATER = DEGC.USER_NAME_LASTUPDATER  
       ,DGC.ID_STATUS_CHAMADO = DEGC.ID_STATUS_CHAMADO  
       ,DGC.STATUS_CHAMADO = DEGC.STATUS_CHAMADO  
       ,DGC.ID_TIPO_CHAMADO = DEGC.ID_TIPO_CHAMADO  
       ,DGC.TIPO_CHAMADO = DEGC.TIPO_CHAMADO  
       ,DGC.ID_CATEGORIA = DEGC.ID_CATEGORIA  
       ,DGC.CATEGORIA = DEGC.CATEGORIA  
       ,DGC.SUBCATEGORIA = DEGC.SUBCATEGORIA  
       ,DGC.ID_REQUERENTE = DEGC.ID_REQUERENTE  
       ,DGC.LOGIN_REQUERENTE = DEGC.LOGIN_REQUERENTE  
       ,DGC.NOME_REQUERENTE = DEGC.NOME_REQUERENTE  
       ,DGC.SETOR_REQUERENTE = DEGC.SETOR_REQUERENTE  
       ,DGC.ID_STATUS_SOLUCAO = DEGC.ID_STATUS_SOLUCAO  
       ,DGC.STATUS_SOLUCAO = DEGC.STATUS_SOLUCAO  
       ,DGC.DATA_SOLUCAO = DEGC.DATA_SOLUCAO  
       ,DGC.ID_ANALISTA_SOLUCAO = DEGC.ID_ANALISTA_SOLUCAO  
       ,DGC.LOGIN_ANALISTA_SOLUCAO = DEGC.LOGIN_ANALISTA_SOLUCAO  
       ,DGC.NOME_ANALISTA_SOLUCAO = DEGC.NOME_ANALISTA_SOLUCAO  
       ,DGC.ID_AVALIADOR_SOLUCAO = DEGC.ID_AVALIADOR_SOLUCAO  
       ,DGC.LOGIN_AVALIADOR_SOLUCAO = DEGC.LOGIN_AVALIADOR_SOLUCAO  
       ,DGC.NOME_AVALIADOR_SOLUCAO = DEGC.NOME_AVALIADOR_SOLUCAO  
       ,DGC.DATA_UPDATE = GETDATE()  
    FROM  
    	 ATLAS.DBO.DBM_GLPI_CHAMADOS DGC WITH(NOLOCK)  
    	 INNER JOIN ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS DEGC WITH(NOLOCK) ON DEGC.ID_CHAMADO = DGC.ID_CHAMADO  
    WHERE  
    	 DGC.DATA_MODIFICACAO_CHAMADO != DEGC.DATA_MODIFICACAO_CHAMADO  
    	 AND DEGC.DATA_COLETA_GLPI = (SELECT MAX(DATA_COLETA_GLPI) FROM ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS DEGC WITH(NOLOCK))  
      
    /* ######################################################################## */  
    /* ####################	   INSERE NOVOS CHAMADOS 	######################### */  
      
    INSERT INTO DBM_GLPI_CHAMADOS (  
    	  ID_CHAMADO  
    	 ,TITULO_CHAMADO  
    	 ,DATA_ABERTURA_CHAMADO  
    	 ,DATA_MODIFICACAO_CHAMADO  
    	 ,DATA_SOLUCAO_CHAMADO  
    	 ,DATA_FECHAMENTO_CHAMADO  
    	 ,SETOR_ATRIBUIDO  
    	 ,NOME_ANALISTA_ATRIBUIDO 
    	 ,LOGIN_ANALISTA_ATRIBUIDO 
    	 ,USER_ID_LASTUPDATER  
    	 ,USER_LOGIN_LASTUPDATER  
    	 ,USER_NAME_LASTUPDATER  
    	 ,ID_STATUS_CHAMADO  
    	 ,STATUS_CHAMADO  
    	 ,ID_TIPO_CHAMADO  
    	 ,TIPO_CHAMADO  
    	 ,ID_CATEGORIA  
    	 ,CATEGORIA  
    	 ,SUBCATEGORIA  
    	 ,ID_REQUERENTE  
    	 ,LOGIN_REQUERENTE  
    	 ,NOME_REQUERENTE  
    	 ,SETOR_REQUERENTE  
    	 ,ID_STATUS_SOLUCAO  
    	 ,STATUS_SOLUCAO  
    	 ,DATA_SOLUCAO  
    	 ,ID_ANALISTA_SOLUCAO  
    	 ,LOGIN_ANALISTA_SOLUCAO  
    	 ,NOME_ANALISTA_SOLUCAO  
    	 ,ID_AVALIADOR_SOLUCAO  
    	 ,LOGIN_AVALIADOR_SOLUCAO  
    	 ,NOME_AVALIADOR_SOLUCAO  
    	 ,DATA_COLETA_GLPI  
    )  
    SELECT  
    	 DEGC.ID_CHAMADO  
    	,DEGC.TITULO_CHAMADO  
    	,DEGC.DATA_ABERTURA_CHAMADO  
    	,DEGC.DATA_MODIFICACAO_CHAMADO  
    	,DEGC.DATA_SOLUCAO_CHAMADO  
    	,DEGC.DATA_FECHAMENTO_CHAMADO  
    	,DEGC.SETOR_ATRIBUIDO  
    	,DEGC.NOME_ANALISTA_ATRIBUIDO  
    	,DEGC.LOGIN_ANALISTA_ATRIBUIDO 
    	,DEGC.USER_ID_LASTUPDATER  
    	,DEGC.USER_LOGIN_LASTUPDATER  
    	,DEGC.USER_NAME_LASTUPDATER  
    	,DEGC.ID_STATUS_CHAMADO  
    	,DEGC.STATUS_CHAMADO  
    	,DEGC.ID_TIPO_CHAMADO  
    	,DEGC.TIPO_CHAMADO  
    	,DEGC.ID_CATEGORIA  
    	,DEGC.CATEGORIA  
    	,DEGC.SUBCATEGORIA  
    	,DEGC.ID_REQUERENTE  
    	,DEGC.LOGIN_REQUERENTE  
    	,DEGC.NOME_REQUERENTE  
    	,DEGC.SETOR_REQUERENTE  
    	,DEGC.ID_STATUS_SOLUCAO  
    	,DEGC.STATUS_SOLUCAO  
    	,DEGC.DATA_SOLUCAO  
    	,DEGC.ID_ANALISTA_SOLUCAO  
    	,DEGC.LOGIN_ANALISTA_SOLUCAO  
    	,DEGC.NOME_ANALISTA_SOLUCAO  
    	,DEGC.ID_AVALIADOR_SOLUCAO  
    	,DEGC.LOGIN_AVALIADOR_SOLUCAO  
    	,DEGC.NOME_AVALIADOR_SOLUCAO  
    	,DEGC.DATA_COLETA_GLPI  
    FROM  
    	ATLAS.DBO.DBM_ETL_GLPI_CHAMADOS DEGC WITH(NOLOCK)  
    	LEFT JOIN ATLAS.DBO.DBM_GLPI_CHAMADOS DGC WITH(NOLOCK) ON DGC.ID_CHAMADO = DEGC.ID_CHAMADO  
    WHERE  
    	DGC.ID_CHAMADO IS NULL  
      
    /* #########################################################################*/  
    /* ######################	TRATAMENTO DE DADOS		############################*/  
      
    -- ## ATUALIZA DATA_UPDATE DOS DADOS INSERIDOS NO PROCESSO DE INSERT DE CHAMADOS ## 
      
    UPDATE ATLAS.DBO.DBM_GLPI_CHAMADOS SET DATA_UPDATE = DATA_INSERT WHERE DATA_UPDATE IS NULL  
      
    -- ## ATUALIZA CAMPOS DE DATA QUE DEVERIAM SER NULOS  ##
      
    UPDATE ATLAS.DBO.DBM_GLPI_CHAMADOS SET DATA_SOLUCAO_CHAMADO = NULL WHERE DATA_SOLUCAO_CHAMADO = '1900-01-01 00:00:00.000'  
      
    UPDATE ATLAS.DBO.DBM_GLPI_CHAMADOS SET DATA_FECHAMENTO_CHAMADO = NULL WHERE DATA_FECHAMENTO_CHAMADO = '1900-01-01 00:00:00.000'  
      
    UPDATE ATLAS.DBO.DBM_GLPI_CHAMADOS SET DATA_SOLUCAO = NULL WHERE DATA_SOLUCAO = '1900-01-01 00:00:00.000'  
      
    -- ## ATUALIZA CAMPO NOME_ANALISTA_ATRIBUIDO PARA BRANCO  ##
      
    UPDATE ATLAS.DBO.DBM_GLPI_CHAMADOS SET NOME_ANALISTA_ATRIBUIDO = '' WHERE NOME_ANALISTA_ATRIBUIDO IS NULL  
      
    -- ## ATUALIZA CAMPO SETOR_REQUERENTE_TRATADO PARA TRATAMENTO  ##
      
    UPDATE ATLAS.DBO.DBM_GLPI_CHAMADOS SET  
      SETOR_REQUERENTE_TRATADO =  
        (CASE   
           WHEN SETOR_REQUERENTE LIKE '%GR Inovação e Eficiência do Negócio%' THEN 'CONTROLADORIA'  
           WHEN SETOR_REQUERENTE LIKE '%MIS%' THEN 'MIS'  
           WHEN SETOR_REQUERENTE LIKE '%Equipe Telecom%' THEN 'TELECOM'  
           WHEN SETOR_REQUERENTE LIKE '%Planejamento Tático%' THEN 'TÁTICO'  
           WHEN SETOR_REQUERENTE LIKE '%Bulk Actions%' THEN 'BULK ACTIONS'  
           WHEN SETOR_REQUERENTE LIKE '%Suporte Fenix%' THEN 'BULK ACTIONS'  
           WHEN SETOR_REQUERENTE LIKE '%Cronos%' THEN 'BULK ACTIONS'  
           WHEN SETOR_REQUERENTE LIKE '%Portais%' THEN 'BULK ACTIONS'  
           WHEN SETOR_REQUERENTE LIKE '%Hermes SMS%' THEN 'BULK ACTIONS'  
           WHEN SETOR_REQUERENTE LIKE '%Suporte Hermes%' THEN 'BULK ACTIONS'  
           WHEN SETOR_REQUERENTE LIKE '%TN%' THEN 'TECNOLOGIA DE NEGÓCIO'  
           WHEN SETOR_REQUERENTE LIKE '%TEC.NEGÓCIO%' THEN 'TECNOLOGIA DE NEGÓCIO'  
           WHEN SETOR_REQUERENTE LIKE '%Projetos Digitais%' THEN 'PROJETOS DIGITAIS'  
           WHEN SETOR_REQUERENTE LIKE '%RH%' THEN 'RH'  
           WHEN SETOR_REQUERENTE LIKE '%Recrutamento e Seleção%' THEN 'RECRUTAMENTO E SELEÇÃO'  
           WHEN SETOR_REQUERENTE LIKE '%Equipe Zeus%' THEN 'ZEUS'  
           WHEN SETOR_REQUERENTE LIKE '%Agente Digital%' THEN 'ZEUS'  
           WHEN SETOR_REQUERENTE LIKE '%Infra/Telecom Zeus%' THEN 'ZEUS'  
           ELSE 'OPERAÇÃO'  
        END)  
    
    "
  )
  },error = function(e){
    msgErro <- paste("mensagem:",e$message)
    writeLog(msgErro)
    dbClearResult(sql)
    stop(stopMsg)
  }
  )
  
  ## Limpa o resultado do dbSendQuery()
  dbClearResult(sql)
  
  print("Encerrando conexão com o banco Atlas")
  
  writeLog("Encerrando conexão com o banco Atlas")
  ## Disconectar do db
  dbDisconnect(conn)
  
  execTime <- paste(
     "[Finalizando]"
    ,"Tempo de execução:"
    ,round(difftime(as.POSIXct(Sys.time(), tz = "UTC"), startTime, units = "secs"))
    ,"segundos"
    ,sep = " "
  )
  
  writeLog(execTime)
  
  print("Liberando memória e objetos")
  
  ## liberar memória
  gc()
  
  ## Remover objetos
  rm(list = ls())
  
  ## Aguardar 300 segundos (5 min) até a próxima execução do programa
  #Sys.sleep(300)

##}
  
print("Fim do programa de ETL")
  
################################################################################
############################     FIM     #######################################
################################################################################

