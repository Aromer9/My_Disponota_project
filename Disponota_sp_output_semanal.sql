Código SP Disponota_sp_output_semanal  
CREATE OR REPLACE PROCEDURE `wmt-edw-sandbox.cl_operations_bi_stg.disponota_sp_output_semanal`() BEGIN
--Delaración de variables
DECLARE fecini DATE;
DECLARE dias_al_sabado_ini INT64;
DECLARE fecini_inv DATE;
DECLARE fecter DATE;
DECLARE dias_al_sabado_ter INT64;
DECLARE fecter_inv DATE;
DECLARE intervalo  INT64;

--Seteo de variables
SET fecini = CURRENT_DATE()-7 ;
SET dias_al_sabado_ini   = 6 - 7 - EXTRACT(DAYOFWEEK FROM  DATE (DATE_ADD(fecini , INTERVAL -1 day )));
SET fecini_inv =   DATE_ADD(fecini , INTERVAL  dias_al_sabado_ini day);
SET fecter = CURRENT_DATE()-1 ;
SET dias_al_sabado_ter   = 6 - 7 - EXTRACT(DAYOFWEEK FROM  DATE (DATE_ADD(fecter , INTERVAL -1 day )));
SET fecter_inv =   DATE_ADD(fecter , INTERVAL  dias_al_sabado_ter day);
SET intervalo = DATE_DIFF(  fecter,  fecini, day);

--Borra tabla full anterior 
delete from  `wmt-edw-sandbox.cl_operations_bi_stg.disponota_output_full_semanal`
where true;
--Inserta los nuevos a la tabla delta_disponota
insert into  `wmt-edw-sandbox.cl_operations_bi_stg.disponota_output_full_semanal`
        (indicador, fecha, kpi, nota, store_nbr, disponota, fecha_ejecucion, relleno_disponota)

select 
    indicador,
    fecha,
    kpi,
    nota,
    store_nbr,
    round(disponota,2) disponota,
    EXTRACT(datetime FROM 
        (SELECT current_timestamp() AS timestamp_value )
        AT TIME ZONE "America/Santiago") AS fecha_ejecucion,
    FALSE AS relleno_disponota
from 
    (

        select
            u.indicador,          
            u.fecha,
            u.kpi,
            u.nota,
            u.store_nbr,
            SUM(peso * u.nota) OVER (PARTITION BY u.fecha, u.store_nbr) disponota
        --into `wmt-edw-sandbox.cl_operations_gte_stg.tmp_new_output_disponota`
            --SUM(pd.nota *pd.peso) OVER(PARTITION BY tb.fecha, tb.store_nbr) AS disponota
        from (

        -- Calculo de NSG 
        -- *** hay que remplazar la tabla master por la vista "master con express 400"
        SELECT
            tb.indicador,          
            tb.FECHACREACION fecha,
            tb.kpi,
            pd.nota,
            tb.STORE_NBR store_nbr,
            pd.peso
            --vdo.id store_nbr,
            --vdo.format_id,
            --pd.cod_formato,

            --SUM(IFNULL(pd.nota,10)*pd.peso) OVER(PARTITION BY tb.FECHACREACION, tb.STORE_NBR) AS disponota
        FROM (
                SELECT          
                FECHACREACION,
                STORE_NBR,
                'NSG' AS indicador,
                100-round(SAFE_DIVIDE(sum(a.NUMERADOR_NSG),sum(a.DENOMINADOR_NSG))*100,2) as kpi
                FROM `wmt-edw-sandbox.cl_operations_inventory_cmp.on_shelf_availability_history_cmp` AS a
                group by a.fechacreacion, a.store_nbr
                UNION ALL
                SELECT          
                FECHACREACION,
                STORE_NBR,
                'QUIEBRES' AS indicador,
                round(SAFE_DIVIDE(SUM(a.REPONER_QUIEBRE),SUM(a.BIN_QTY))*100,2) as kpi
                FROM `wmt-edw-sandbox.cl_operations_inventory_cmp.on_shelf_availability_history_cmp` AS a
                group by a.fechacreacion, a.store_nbr
                UNION ALL
                SELECT          
                FECHACREACION,
                STORE_NBR,
                'LLENAR GONDOLAS' AS indicador,
                round(SAFE_DIVIDE(SUM(a.LLENAR_GONDOLA), SUM(a.BIN_QTY))*100,2) as kpi
                FROM `wmt-edw-sandbox.cl_operations_inventory_cmp.on_shelf_availability_history_cmp` AS a
                group by a.fechacreacion, a.store_nbr
                UNION ALL
                SELECT          
                FECHACREACION,
                STORE_NBR,
                'INCONSISTENCIAS' AS indicador,
                round(SAFE_DIVIDE(SUM(a.BIN_INCONSISTENCIAS), SUM(a.COM_BINS))*100,2) as kpi
                FROM `wmt-edw-sandbox.cl_operations_inventory_cmp.on_shelf_availability_history_cmp` AS a 
                group by a.fechacreacion, a.store_nbr
                UNION ALL
                SELECT          
                FECHACREACION,
                STORE_NBR,
                'STOCK BINEO' AS indicador,
                round(SAFE_DIVIDE(SUM(a.BIN_QTY), SUM(a.BACKROOM))*100,2) as kpi
                FROM `wmt-edw-sandbox.cl_operations_inventory_cmp.on_shelf_availability_history_cmp` AS a
                group by a.fechacreacion, a.store_nbr
				
				--Indicadores de AP              
                
                UNION ALL				
                SELECT 
                  MY_DATE FECHACREACION,
                  --DATE_ADD(MY_DATE,INTERVAL dias_al_sabado DAY) cut_date,
                  store_nbr,
                  indicador,
                  kpi
                FROM (
                SELECT
                    MY_DATE,                 
                      6 - 7 - EXTRACT(DAYOFWEEK FROM  DATE (DATE_ADD(MY_DATE , INTERVAL -1 day )))      dias_al_sabado    
                  FROM 
                  (
                  SELECT DATE_ADD( fecini ,INTERVAL param DAY) AS MY_DATE
                    FROM unnest(GENERATE_ARRAY(0, intervalo, 1)) as param
                  )
                ) AS C
                INNER JOIN 
                  (SELECT          
                    CUT_DATE,
                    store_nbr,
                    'PI' AS indicador,
                    ROUND(SAFE_DIVIDE(SUM(a.EXACT_PI_COUNT),  
                    SUM(a.COUNT_))*100,2) as kpi
                  FROM `wmt-edw-sandbox.PR_WMCLAP.VW_CAD_RESUME_RESULT` AS a
                  where IS_DISPONOTA_PUBLISH = 1
                  group by a.CUT_DATE, a.STORE_NBR
                  UNION ALL
                  SELECT          
                  CUT_DATE,
                  store_nbr,
                  'EXACTITUD PI' AS indicador,
                  round(SAFE_DIVIDE(SUM(a.EXACT_IGUAL_SUM), SUM(a.COUNT_))*100,2) as kpi
                  FROM `wmt-edw-sandbox.PR_WMCLAP.VW_CAD_RESUME_RESULT` AS a
                  where IS_DISPONOTA_PUBLISH = 1
                  group by a.CUT_DATE, a.STORE_NBR
                  UNION ALL
                  SELECT          
                  CUT_DATE,
                  store_nbr,
                  'ADHERENCIA CAT' AS indicador,
                  round(SAFE_DIVIDE(SUM(a.ADHERENCIA_SUM), SUM(a.COUNT_))*100,2) as kpi
                  FROM `wmt-edw-sandbox.PR_WMCLAP.VW_CAD_RESUME_RESULT` AS a
                  where IS_DISPONOTA_PUBLISH = 1
                  group by a.CUT_DATE, a.STORE_NBR
                ) AS AP
                ON DATE_ADD(MY_DATE,INTERVAL dias_al_sabado DAY) = AP.cut_date
				--Fin indicadores de AP

                UNION ALL
                --Modulares desde PDW
                SELECT
                load_date AS fecha,
                store_nbr,
                'MODULARES' AS indicador,
                CASE WHEN modulares = 0 THEN 100
                    WHEN round(SAFE_DIVIDE(SUM(mc.confirmados), 
                                    CASE WHEN SUM(mc.modulares) = 0 THEN 1 ELSE modulares END) *100,2) > 100 THEN 100
                    ELSE round(SAFE_DIVIDE(SUM(mc.confirmados), 
                                    CASE WHEN SUM(mc.modulares) = 0 THEN 1 ELSE modulares END) *100,2) END 
                    AS kpi
                FROM `wmt-edw-sandbox.cl_operations_bi.disponota_pdw_ft_modular_cat` AS mc
                WHERE load_date >= fecini AND load_date <= fecter
                and load_date <= '2022-10-09' -- hasta esta fecha los modulares son de PDW
                AND EXISTS (SELECT * FROM `wmt-edw-sandbox.cl_operations_stores_cmp.master` AS dl 
                            WHERE mc.store_nbr = dl.id and dl.status =    'OPERATING')
                GROUP BY 
                confirmados, 
                modulares, 
                load_date, 
                store_nbr 
                --Fin de Modulares desde PDW
                            
                --Modulares desde BigQuery
                
                UNION ALL
                SELECT                 
                load_ts AS fecha,
                store_nbr,
                'MODULARES' AS indicador,
                CASE WHEN modulares = 0 THEN 100
                    WHEN round(SAFE_DIVIDE(SUM(mc.confirmados), 
                                    CASE WHEN SUM(mc.modulares) = 0 THEN 1 ELSE modulares END) *100,2) > 100 THEN 100
                    ELSE round(SAFE_DIVIDE(SUM(mc.confirmados), 
                                    CASE WHEN SUM(mc.modulares) = 0 THEN 1 ELSE modulares END) *100,2) END 
                    AS kpi
                FROM 
                (  SELECT          
                    load_ts,
                    store_nbr,
                    SUM(CASE WHEN DATE_DIFF(load_ts,modular_eff_date, DAY) >= 7 AND status_code = 7 THEN 1 ELSE 0 END) AS confirmados,
                    SUM(CASE WHEN DATE_DIFF(load_ts,modular_eff_date, DAY) >= 7 THEN 1 ELSE 0 END) AS modulares
                FROM `wmt-edw-sandbox.cl_operations_bi_cmp.ft_modular_cat`
                GROUP BY
                    load_ts, 
                    store_nbr 
                )

                AS mc
                WHERE load_ts >= fecini AND load_ts <= fecter
                and load_ts >= '2022-10-10' --desde esta fecha modulares corre desde bigquery
                AND EXISTS (SELECT * FROM `wmt-edw-sandbox.cl_operations_stores_cmp.master` AS dl 
                            WHERE mc.store_nbr = dl.id and dl.status =    'OPERATING')
                GROUP BY 
                confirmados, 
                modulares, 
                load_ts, 
                store_nbr 
                --Fin Modulares desde BigQuery


            

            ) AS tb
        INNER JOIN 
        `wmt-edw-sandbox.cl_operations_bi.disponota_master_express400` AS vdo
        ON tb.store_nbr = vdo.id
        LEFT JOIN 
        `wmt-edw-sandbox.cl_operations_bi.disponota_kpi_parametros`  AS pd 
            ON  vdo.cod_formato_old = pd.cod_formato 
            AND tb.kpi >= pd.r_min 
            AND tb.kpi <= pd.r_max 
            AND tb.indicador = pd.kpi
        WHERE FECHACREACION BETWEEN fecini AND fecter
        AND vdo.status = 'OPERATING'

        
        ) AS u
    ) AS a; 

END
