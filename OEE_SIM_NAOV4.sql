
WITH ranked_tasks_fim AS (
    SELECT 
        ks.reportdate,
        ks.siteid,
        am.firstname || ' ' || am.lastname AS colaborador,
        sg.subgroup,
        wt.workTeamName,
        ks.userid,
        ks.machineid,  -- ADICIONADO
        ks.stoptime,
        SUM(ks.actualseconds) AS disponivel,
        ROW_NUMBER() OVER (
            PARTITION BY ks.userid, ks.reportdate, ks.machineid  -- AGRUPAMENTO POR EQUIPAMENTO
            ORDER BY ks.stoptime DESC
        ) AS rn
    FROM daas.v1.assignments ks
    JOIN daas.v1.users lu ON ks.userid = lu.userid AND lu.supervisor IS NOT NULL
    JOIN daas.v1.users lu2 ON lu.supervisor = lu2.userid AND lu2.supervisor IS NOT NULL
    JOIN daas.v1.addresses am ON lu.userid = am.addressname AND lu.addressid = am.addressid
    JOIN daas.v1.subgroups sg ON lu.userGroup = sg.userGroup
    JOIN daas.v1.workteams wt ON lu.workTeam = wt.workTeam
    WHERE ks.supervisor <> 'SUPER'
    GROUP BY ks.reportdate, ks.siteid, sg.subgroup, am.firstname, am.lastname,
             wt.workTeamName, ks.userid, ks.stoptime, ks.machineid
),
ranked_tasks_inicio AS (
    SELECT 
        ks.reportdate,
        ks.siteid,
        am.firstname || ' ' || am.lastname AS colaborador,
        sg.subgroup,
        wt.workTeamName,
        ks.userid,
        ks.machineid,  -- ADICIONADO
        ks.starttime,
        SUM(ks.actualseconds) AS disponivel,
        ROW_NUMBER() OVER (
            PARTITION BY ks.userid, ks.reportdate, ks.machineid  -- AGRUPAMENTO POR EQUIPAMENTO
            ORDER BY ks.starttime ASC
        ) AS rn
    FROM daas.v1.assignments ks
    JOIN daas.v1.users lu ON ks.userid = lu.userid AND lu.supervisor IS NOT NULL
    JOIN daas.v1.users lu2 ON lu.supervisor = lu2.userid AND lu2.supervisor IS NOT NULL
    JOIN daas.v1.addresses am ON lu.userid = am.addressname AND lu.addressid = am.addressid
    JOIN daas.v1.subgroups sg ON lu.userGroup = sg.userGroup
    JOIN daas.v1.workteams wt ON lu.workTeam = wt.workTeam
    WHERE ks.supervisor <> 'SUPER'
    GROUP BY ks.reportdate, ks.siteid, sg.subgroup, am.firstname, am.lastname,
             wt.workTeamName, ks.userid, ks.starttime, ks.machineid
),
tarefas_com_HE AS (
    SELECT 
        rtf.reportdate,
        rtf.userid,
        rtf.siteid,
        rtf.subgroup,
        rtf.colaborador,
        rtf.workTeamName,
        rtf.machineid,  -- ADICIONADO
        rtf.disponivel,
        rti.starttime,
        rtf.stoptime,
        CASE 
            WHEN rti.workTeamName = 'TA' AND (
                DATEDIFF(SECOND, DATEADD(HOUR, 14, CAST(rtf.stoptime AS DATE)), rtf.stoptime) > 1800
                OR DATEDIFF(SECOND, rti.starttime, DATEADD(HOUR, 6, CAST(rti.starttime AS DATE))) > 1800
            ) THEN 'Sim'
            WHEN rti.workTeamName = 'TB' AND (
                DATEDIFF(SECOND, DATEADD(HOUR, 22, CAST(rtf.stoptime AS DATE)), rtf.stoptime) > 1800
                OR DATEDIFF(SECOND, rti.starttime, DATEADD(HOUR, 14, CAST(rti.starttime AS DATE))) > 1800
            ) THEN 'Sim'
            WHEN rti.workTeamName = 'TC' AND (
                DATEDIFF(SECOND, DATEADD(MINUTE, 30, DATEADD(HOUR, 6, DATEADD(DAY, 1, CAST(rtf.stoptime AS DATE)))), rtf.stoptime) > 0
                OR DATEDIFF(SECOND, rti.starttime, DATEADD(HOUR, 22, CAST(rti.starttime AS DATE))) > 1800
            ) THEN 'Sim'
            ELSE 'Não'
        END AS FezHE
    FROM ranked_tasks_fim rtf
    JOIN ranked_tasks_inicio rti 
        ON rtf.userid = rti.userid 
        AND rtf.reportdate = rti.reportdate
        AND rtf.machineid = rti.machineid  -- GARANTIR MESMO EQUIPAMENTO
    WHERE rtf.rn = 1 AND rti.rn = 1
)
SELECT 
    TO_CHAR(reportdate, 'DD/MM/YYYY') AS reportdate,
    TO_CHAR(reportdate, 'DD/MM/YYYY') || userid || machineid AS chave,  -- CHAVE ÚNICA COM EQUIPAMENTO
    CASE 
        WHEN siteid = '7323' THEN 'Camacari'
        WHEN siteid = '7026' THEN 'CDMA'
    END AS planta,
    subgroup,
    colaborador,
    workTeamName,
    machineid AS equipamento,  -- NOVA COLUNA
    userid AS login,
    TO_CHAR(starttime, 'HH24:MI') AS inicio_primeira_tarefa,
    TO_CHAR(stoptime, 'HH24:MI') AS fim_ultima_tarefa,
    FezHE,
    CASE 
        WHEN workTeamName = 'TA' THEN
            CASE 
                WHEN starttime < DATEADD(MINUTE, -30, DATEADD(HOUR, 6, CAST(reportdate AS TIMESTAMP))) 
                     AND stoptime > DATEADD(MINUTE, 30, DATEADD(HOUR, 14, CAST(reportdate AS TIMESTAMP))) THEN 'HE no início e fim'
                WHEN starttime < DATEADD(MINUTE, -30, DATEADD(HOUR, 6, CAST(reportdate AS TIMESTAMP))) THEN 'HE no início'
                WHEN stoptime > DATEADD(MINUTE, 30, DATEADD(HOUR, 14, CAST(reportdate AS TIMESTAMP))) THEN 'HE no fim'
                ELSE 'Não'
            END
        WHEN workTeamName = 'TB' THEN
            CASE 
                WHEN starttime < DATEADD(MINUTE, -30, DATEADD(HOUR, 14, CAST(reportdate AS TIMESTAMP))) 
                     AND stoptime > DATEADD(MINUTE, 30, DATEADD(HOUR, 22, CAST(reportdate AS TIMESTAMP))) THEN 'HE no início e fim'
                WHEN starttime < DATEADD(MINUTE, -30, DATEADD(HOUR, 14, CAST(reportdate AS TIMESTAMP))) THEN 'HE no início'
                WHEN stoptime > DATEADD(MINUTE, 30, DATEADD(HOUR, 22, CAST(reportdate AS TIMESTAMP))) THEN 'HE no fim'
                ELSE 'Não'
            END
        WHEN workTeamName = 'TC' THEN
            CASE 
                WHEN starttime < DATEADD(MINUTE, -30, DATEADD(HOUR, 22, CAST(reportdate AS TIMESTAMP))) 
                     AND stoptime > DATEADD(MINUTE, 30, DATEADD(HOUR, 6, DATEADD(DAY, 1, CAST(reportdate AS TIMESTAMP)))) THEN 'HE no início e fim'
                WHEN starttime < DATEADD(MINUTE, -30, DATEADD(HOUR, 22, CAST(reportdate AS TIMESTAMP))) THEN 'HE no início'
                WHEN stoptime > DATEADD(MINUTE, 30, DATEADD(HOUR, 6, DATEADD(DAY, 1, CAST(reportdate AS TIMESTAMP)))) THEN 'HE no fim'
                ELSE 'Não'
            END
        ELSE 'Não'
    END AS hora_extra_inicio_fim
FROM tarefas_com_HE
WHERE FezHE = 'Sim';
