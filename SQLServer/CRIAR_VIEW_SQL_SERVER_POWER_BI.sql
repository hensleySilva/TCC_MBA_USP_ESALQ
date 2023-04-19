USE ATLAS

-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

/*	GRUPOS USUARIOS - VIEW	*/
CREATE VIEW DBO.VW_DBM_GLPI_GRUPOS_USUARIOS AS
SELECT 
	 *
	,UPPER(CASE
			WHEN GGU.LOGIN_USUARIO IN (
				 'SILVA1.HENSLEY'
				,'SANTOS4.MATHEUS'
				,'SANTOS1.LILIAN'
				,'LEITAO.GUSTAVO'
				,'BESSA.IVAN'
				,'CORREA.EMERSON'
				,'RAMOS.JULIANO'
				,'junior.valdecir'
			) THEN 'TN.DBM'
			WHEN GGU.LOGIN_USUARIO IN (
				 'RODRIGUES.RODOLFO'
				,'BITTENCOURT.ANDREA'
				,'SILVA.JONAS'
				,'ARAUJO.JIFFERSON'
				,'GOMES4.LUCAS'
				,'PROCOPIO.ALMIR'
				,'SANTOS.FLORISVALDO'
				,'GIOVANI.SILVA'
				,'MACHADO1.IGOR'
				,'UMEZO.LARISSA'
				,'SANTOS.FLORISVALDO'
				,'GUIMARAES.SANDRO'		
				,'ANDRADE.ALEXSANDRO'
				,'MARTINES.LUCAS'
				,'RIBEIRO1.WILLIAN'
				,'SILVA.GIOVANI'
				,'SUHETT.GABRIEL'
				,'SANTOS.EDGARD'
				,'ARGUS.VITOR'
			) THEN 'TN.ATLAS'
			WHEN GGU.LOGIN_USUARIO IN (
				 'LIMA.VILMAR'
				,'SANTOS.EMANUEL'
				,'SANTOS1.RODRIGO'
				,'OLIVEIRA5.VIN�CIUS'
				,'SANTANDER.LUIZ'
				,'OLIVEIRA1.PEDRO'
				,'OLIVEIRA.LEONIDAS'
				,'AURELIO.DOUGLAS'
				,'SOUZA.ISRAEL'
				,'COSTA6.LUCAS'
				,'YAMAZAKI.EDUARDO'	
				,'TORRES.KAUAN'
				,'NOVAIS.GLEIDISON'
				,'TOYODA.FELIPE'
				,'MARTINS1.EDUARDA'
				,'SILVA13.AMANDA'
			) THEN 'TN.PROCESSOS'
			WHEN GGU.LOGIN_USUARIO IN (
				  'FILHO.AUGUSTO'	
				 ,'MARQUES.DANIEL'
				 ,'TEIXEIRA1.RAFAEL'
			) THEN 'TN.DBA'
			WHEN GGU.LOGIN_USUARIO IN (
				  'LIMA.FRANKLIN'	
				 ,'MOLINARI.LUIS'
				 ,'PEREIRA3.PRISCILA'
				 ,'ARAUJO5.ANA'
				 ,'SCHIMANKO.GABRIEL'
				 ,'SANTOS12.MATHEUS'
				 ,'PIRES.MARINA'
				 ,'HORNUNG.KETLYN'
				 ,'MOLINA.ROBERVAL'
				 ,'EVARISTO.KAUE'
				 ,'PROBST.RAFAEL'
			) THEN 'TN.SUPORTE'
			WHEN GGU.LOGIN_USUARIO IN (
				  'LOURENCO.WIVIAN'	
			) THEN 'TN.CONTROLLER'
		END) AS NOME_GRUPO_ANALISTA
	,(CASE 
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TEC.NEG�CIO%' THEN 'DANILO SOUZA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%MIS%' THEN 'DARIO COSTA DA SILVA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%CONTROL%' THEN 'DARIO COSTA DA SILVA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%T�TICO%' THEN 'DARIO COSTA DA SILVA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%BULK ACTIONS%' THEN 'DARIO COSTA DA SILVA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TELECOM%' THEN 'DARIO COSTA DA SILVA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%GR INOVA��O E EFICI�NCIA DO NEG�CIO%' THEN 'KAMILA PEREIRA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%RH%' THEN 'VIVIAN BUENO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%ZEUS%' THEN 'RAFAEL AGUIAR FERREIRA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%FENIX%' THEN 'RAFAEL AGUIAR FERREIRA'
	END) AS NOME_HEAD_GRUPO
	,(CASE
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.ATENA%' THEN 'RODOLFO RODRIGUES SIM�ES ARA�JO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.ATLANTA%' THEN 'RODOLFO RODRIGUES SIM�ES ARA�JO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.ATLAS%' THEN 'RODOLFO RODRIGUES SIM�ES ARA�JO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.DBA%' THEN 'JEFFERSON SANTOS'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.DBM%' THEN 'RODOLFO RODRIGUES SIM�ES ARA�JO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.FERRAMENTAS%' THEN 'JEFFERSON SANTOS'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.IMPLANTA��O%' THEN 'JEFFERSON SANTOS'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.PROCESSOS%' THEN 'JEFFERSON SANTOS'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TN.SUPORTE%' THEN 'JEFFERSON SANTOS'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%MIS%' THEN 'FELIPE  BORGES'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%CONTROL SAC%' THEN 'RODRIGO VIEIRA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%CONTROL%' THEN 'RUBENS RANGINHA MACEDO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%T�TICO%' THEN 'JONATHAN FEITOSA'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%BULK ACTIONS%' THEN 'ANDRESSA PROBST'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%TELECOM%' THEN 'LORENA BALBINO'
		WHEN GGU.NOME_COMPLETO_GRUPO LIKE '%GR INOVA��O E EFICI�NCIA DO NEG�CIO%' THEN 'KAMILLA PEREIRA'
	END) AS NOME_GERENTE_GRUPO
FROM
	ATLAS.DBO.DBM_GLPI_GRUPOS_USUARIOS GGU WITH(NOLOCK)

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

CREATE VIEW DBO.VW_DBM_GLPI_CHAMADOS_SETORES AS
SELECT
	 C.ID_CHAMADO
	,C.DATA_ABERTURA_CHAMADO
	,C.DATA_FECHAMENTO_CHAMADO
	,C.DATA_MODIFICACAO_CHAMADO
	,C.SETOR_REQUERENTE_TRATADO
	,UPPER(
		REPLACE(
			ISNULL(CHAMADOS_ATRIBUIDOS.SETOR_ATRIBUIDO,'(N�O ATRIBU�DO)')
			,'TN.PROCESSOS (MCKINSEY)'
			,'TN.PROCESSOS'
		)
	)  AS SETOR_ATRIBUIDO
	,CHAMADOS_ATRIBUIDOS.ORIGEM_ATRIBUICAO AS ORIGEM_ATRIBUICAO_SETOR
	,C.STATUS_CHAMADO
FROM
	DBM_GLPI_CHAMADOS C WITH(NOLOCK)
	LEFT JOIN (
		/*	CATEGORIA	*/
		SELECT *
		FROM (
			SELECT
				 C.ID_CHAMADO
				,(CASE
					WHEN CATEGORIA LIKE '%TN.DBA%'			THEN 'TN.DBA'		
					WHEN CATEGORIA LIKE '%TN.ATENA%'		THEN 'TN.ATENA'		
					WHEN CATEGORIA LIKE '%TN.ATLANTA%'		THEN 'TN.ATLANTA'	
					WHEN CATEGORIA LIKE '%TN.ATLAS%'		THEN 'TN.ATLAS'		
					WHEN CATEGORIA LIKE '%TN.DBA%'			THEN 'TN.DBA'		
					WHEN CATEGORIA LIKE '%TN.DBM%'			THEN 'TN.DBM'		
					WHEN CATEGORIA LIKE '%TN.FERRAMENTAS%'	THEN 'TN.FERRAMENTAS'
					WHEN CATEGORIA LIKE '%TN.IMPLANTA��O%'	THEN 'TN.IMPLANTA��O'
					WHEN CATEGORIA LIKE '%TN.PROCESSOS%'	THEN 'TN.PROCESSOS'	
					WHEN CATEGORIA LIKE '%TN.SUPORTE%'		THEN 'TN.SUPORTE'	
				END) AS SETOR_ATRIBUIDO
				,'CATEGORIA' AS ORIGEM_ATRIBUICAO
			FROM
				DBM_GLPI_CHAMADOS C WITH(NOLOCK)
		) AS C
		WHERE C.SETOR_ATRIBUIDO IS NOT NULL

		UNION ALL

		/*	SETOR_ATRIBUIDO	*/
		SELECT
			 C.ID_CHAMADO
			,VALUE AS SETOR_ATRIBUIDO
			,'SETOR_ATRIBUIDO' AS ORIGEM_ATRIBUICAO
		FROM
			DBM_GLPI_CHAMADOS C WITH(NOLOCK)
			OUTER APPLY STRING_SPLIT(SETOR_ATRIBUIDO, ',')
		WHERE
			C.SETOR_ATRIBUIDO != '(N�O ATRIBU�DO)'

		UNION ALL

		/*	ANALISTA_ATRIBUIDO	*/
		SELECT *
		FROM (
			SELECT
				 C.ID_CHAMADO
				,GGU.NOME_GRUPO_ANALISTA AS SETOR_ATRIBUIDO
				,'ANALISTA_ATRIBUIDO' AS ORIGEM_ATRIBUICAO
			FROM
				DBM_GLPI_CHAMADOS C WITH(NOLOCK)
				OUTER APPLY STRING_SPLIT(LOGIN_ANALISTA_ATRIBUIDO, ',')
				LEFT JOIN (
					SELECT DISTINCT
						 LOGIN_USUARIO
						,NOME_GRUPO_ANALISTA
					FROM
						DBO.VW_DBM_GLPI_GRUPOS_USUARIOS GGU WITH(NOLOCK)
					WHERE
						NOME_GRUPO_ANALISTA IS NOT NULL
				)GGU ON GGU.LOGIN_USUARIO = VALUE
			WHERE
				C.LOGIN_ANALISTA_ATRIBUIDO != '(N�O ATRIBU�DO)'
		) AS SQ
		WHERE SQ.SETOR_ATRIBUIDO IS NOT NULL
	) CHAMADOS_ATRIBUIDOS ON CHAMADOS_ATRIBUIDOS.ID_CHAMADO = C.ID_CHAMADO


--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

CREATE VIEW DBO.VW_DBM_GLPI_CHAMADOS_ANALISTAS AS
SELECT DISTINCT
	 ID_CHAMADO
	,c.TITULO_CHAMADO
	,C.DATA_ABERTURA_CHAMADO
	,C.DATA_FECHAMENTO_CHAMADO
	,C.DATA_MODIFICACAO_CHAMADO
	,C.STATUS_CHAMADO
	,C.SETOR_REQUERENTE_TRATADO
	,VALUE LOGIN_ANALISTA_ATRIBUIDO
	,substring(GGU.NOME_USUARIO,1,CHARINDEX(' ',GGU.NOME_USUARIO)) NOME_USUARIO
	,GGU.NOME_GRUPO_ANALISTA AS  NOME_GRUPO_ANALISTA
FROM
	DBM_GLPI_CHAMADOS C WITH(NOLOCK) 
	CROSS APPLY STRING_SPLIT(LOGIN_ANALISTA_ATRIBUIDO, ',')
	INNER JOIN  DBO.VW_DBM_GLPI_GRUPOS_USUARIOS GGU WITH(NOLOCK)
		ON GGU.LOGIN_USUARIO = VALUE
WHERE
	LOGIN_ANALISTA_ATRIBUIDO != '(N�o atribu�do)'

UNION ALL

SELECT DISTINCT
	 C.ID_CHAMADO
	,C.TITULO_CHAMADO
	,C.DATA_ABERTURA_CHAMADO
	,C.DATA_FECHAMENTO_CHAMADO
	,C.DATA_MODIFICACAO_CHAMADO
	,C.STATUS_CHAMADO
	,C.SETOR_REQUERENTE_TRATADO
	,C.LOGIN_ANALISTA_ATRIBUIDO
	,C.NOME_ANALISTA_ATRIBUIDO AS NOME_USUARIO
	,GCS.SETOR_ATRIBUIDO AS  NOME_GRUPO_ANALISTA
FROM
	DBM_GLPI_CHAMADOS C WITH(NOLOCK) 
	INNER JOIN DBO.VW_DBM_GLPI_CHAMADOS_SETORES GCS WITH(NOLOCK)
		ON GCS.ID_CHAMADO = C.ID_CHAMADO
WHERE
	LOGIN_ANALISTA_ATRIBUIDO = '(N�o atribu�do)'
