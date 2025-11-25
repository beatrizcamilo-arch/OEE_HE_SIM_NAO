-- 1. CTE para pegar a última tarefa do dia por colaborador
WITH ranked_tasks_fim AS (
    SELECT 
        ks.reportdate,
        ks.siteid,
        ks.machineid,
        am.firstname || ' ' || am.lastname AS colaborador,
        am2.firstname || ' ' || am2.lastname AS lideranca,
        sg.subgroup,
        wt.workTeamName,
        ks.userid,
        ks.stoptime,
        SUM(ks.actualseconds) AS disponivel,
        
        ROW_NUMBER() OVER (
            PARTITION BY ks.userid, ks.reportdate
            ORDER BY ks.stoptime DESC
        ) AS rn
    FROM daas.v1.assignments ks
    JOIN daas.v1.users lu ON ks.userid = lu.userid AND lu.supervisor IS NOT NULL
    JOIN daas.v1.users lu2 ON lu.supervisor = lu2.userid AND lu2.supervisor IS NOT NULL
    JOIN daas.v1.addresses am ON lu.userid = am.addressname AND lu.addressid = am.addressid
    JOIN daas.v1.addresses am2 ON lu2.userid = am2.addressname AND lu2.addressid = am2.addressid
    JOIN daas.v1.subgroups sg ON lu.userGroup = sg.userGroup
    JOIN daas.v1.workteams wt ON lu.workTeam = wt.workTeam
    WHERE ks.supervisor <> 'SUPER'
    GROUP BY ks.reportdate, ks.siteid, sg.subgroup, am.firstname, am.lastname,
             am2.firstname, am2.lastname, wt.workTeamName, ks.userid, ks.stoptime, ks.machineid
),

-- 2. CTE para pegar a primeira tarefa do dia por colaborador
ranked_tasks_inicio AS (
    SELECT 
        ks.reportdate,
        ks.siteid,
        ks.machineid,
        am.firstname || ' ' || am.lastname AS colaborador,
        am2.firstname || ' ' || am2.lastname AS lideranca,
        sg.subgroup,
        wt.workTeamName,
        ks.userid,
        ks.starttime,
        SUM(ks.actualseconds) AS disponivel,
        
        ROW_NUMBER() OVER (
            PARTITION BY ks.userid, ks.reportdate
            ORDER BY ks.starttime ASC
        ) AS rn
    FROM daas.v1.assignments ks
    JOIN daas.v1.users lu ON ks.userid = lu.userid AND lu.supervisor IS NOT NULL
    JOIN daas.v1.users lu2 ON lu.supervisor = lu2.userid AND lu2.supervisor IS NOT NULL
    JOIN daas.v1.addresses am ON lu.userid = am.addressname AND lu.addressid = am.addressid
    JOIN daas.v1.addresses am2 ON lu2.userid = am2.addressname AND lu2.addressid = am2.addressid
    JOIN daas.v1.subgroups sg ON lu.userGroup = sg.userGroup
    JOIN daas.v1.workteams wt ON lu.workTeam = wt.workTeam
    WHERE ks.supervisor <> 'SUPER'
    GROUP BY ks.reportdate, ks.siteid, sg.subgroup, am.firstname, am.lastname,
             am2.firstname, am2.lastname, wt.workTeamName, ks.userid, ks.starttime, ks.machineid
),

-- 3. CTE que junta as duas e calcula FezHE
tarefas_com_HE AS (
    SELECT 
        rtf.reportdate,
        rtf.userid,
        rtf.siteid,
        rtf.subgroup,
        rtf.colaborador,
        rtf.lideranca,
        rtf.workTeamName,
        rtf.machineid,
        rtf.disponivel,
        rti.starttime,
        rtf.stoptime,
        
        -- Lógica para identificar se fez hora extra
      
CASE 
    WHEN rti.workTeamName = 'TA' AND (
        DATEDIFF(SECOND, DATEADD(MINUTE, 30, DATEADD(HOUR, 14, CAST(CAST(rtf.stoptime AS DATE) AS DATETIME))), rtf.stoptime) > 0
        OR DATEDIFF(SECOND, rti.starttime, DATEADD(HOUR, 6, CAST(rti.starttime AS DATE))) > 0
    ) THEN 'Sim'
    WHEN rti.workTeamName = 'TB' AND (
        DATEDIFF(SECOND, DATEADD(MINUTE, 30, DATEADD(HOUR, 22, CAST(CAST(rtf.stoptime AS DATE) AS DATETIME))), rtf.stoptime) > 0
        OR DATEDIFF(SECOND, rti.starttime, DATEADD(HOUR, 14, CAST(rti.starttime AS DATE))) > 0
    ) THEN 'Sim'
    WHEN rti.workTeamName = 'TC' AND (
        DATEDIFF(SECOND, DATEADD(MINUTE, 30, DATEADD(HOUR, 06, CAST(CAST(rtf.stoptime AS DATE) AS DATETIME))), 
            CASE WHEN rtf.stoptime <= DATEADD(HOUR, 08, CAST(rtf.stoptime AS DATE)) THEN rtf.stoptime END
        ) > 0
        OR DATEDIFF(SECOND, rti.starttime, 
            CASE WHEN rti.starttime >= DATEADD(HOUR, 20, CAST(rti.starttime AS DATE)) THEN DATEADD(HOUR, 22, CAST(rti.starttime AS DATE)) END
        ) > 0
    ) THEN 'Sim'
    ELSE 'Não'
END AS FezHE

    FROM ranked_tasks_fim rtf
    JOIN ranked_tasks_inicio rti ON rtf.userid = rti.userid AND rtf.reportdate = rti.reportdate
    WHERE rtf.rn = 1 AND rti.rn = 1
)

-- 4. SELECT final com formatação e filtro FezHE = 'Sim'
SELECT 
    TO_CHAR(reportdate, 'DD/MM/YYYY') AS reportdate,
    TO_CHAR(reportdate, 'DD/MM/YYYY') || userid AS chave,
    
    CASE 
        WHEN siteid = '7323' THEN 'Camacari'
        WHEN siteid = '7026' THEN 'CDMA'
    END AS planta,
    
    subgroup,
    colaborador,
    lideranca,
    workTeamName,
    userid AS login,
    machineid,
    
    disponivel AS tempo_total_segundos,
    CAST(disponivel AS FLOAT) / 3600 AS tempo_total_hrs,
    
    TO_CHAR(starttime, 'HH24:MI') AS inicio_primeira_tarefa,
    TO_CHAR(stoptime, 'HH24:MI') AS fim_ultima_tarefa,
    
    FezHE
FROM tarefas_com_HE
WHERE FezHE = 'Sim';
