#!/bin/bash

#--------------------------------------------------------------------------
# sprof version 0.5b - Feb 2024
# vim: et:ts=4:sw=4:sm:ai:
#--------------------------------------------------------------------------

#---------------------------------------------------------------------------
# Setting Default Values
#---------------------------------------------------------------------------
SPV="sprof 0.5b"
OUT=sprof.out
GZP=0
VMON=v_monitor  # Vertica monitor  schema
VINT=v_internal # Vertica internal schema
VCAT=v_catalog  # Vertica catalog schema


SDATE=0001-01-01
EDATE=9999-12-31

MIN_VVERSION=9.3 # earliest supported version for that script

#GRASP Querying params
NOTGRASP=""

usage="Usage: sprof [-o | --output out_file] [-g |--gzip] [-c schema] [-m schema] [-i schema] [-S start] [-E end] [-h|--help]\n"
usage+="    -o | --output out_file: defines output file (default sprof.out)\n"
usage+="    -g | --gzip: gzips output file)\n"
usage+="    -m schema: defines monitoring schema (default ${VMON})\n"
usage+="    -i schema: defines dc_tables schema (default ${VINT})\n"
usage+="    -c schema: defines catalog schema (default ${VCAT})\n"
usage+="    -S YYYY-MM-DD: defines start date (default ${SDATE})\n"
usage+="    -E YYYY-MM-DD: defines end date (default ${EDATE})\n"
usage+="    -I YYYYMMDDHHMISS | --grasp YYYYMMDDHHMISS: set scrutin id for grasp\n"
usage+="    -h | --help: prints this message"
usage+=" \n\n"
usage+="Note:\n"
usage+="-------\n"
usage+="    Set your VSQL variables as needed before using sprof:\n\t\tVSQL_USER, \n\t\tVSQL_PASSWORD, \n\t\tVSQL_HOST, \n\t\tVSQL_DATABASE, \n\tand others as applicable.\n"
usage+="    Setting that variables allows you also to connect to remote Vertica DB, if VSQL client is installed locally.\n"


#---------------------------------------------------------------------------
# Command line options handling
#---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        "--output" | "-o")
            OUT=$2
            shift 2
           ;;
        "--gzip" | "-g")
            GZP=1
            shift
            ;;
        "-m")
            VMON=$2
            shift 2
            ;;
        "-i")
            VINT=$2
            shift 2
            ;;
        "-c")
            VCAT=$2
            shift 2
            ;;
        "-S")
            SDATE=$2
            shift 2
            ;;
        "-E")
            EDATE=$2
            shift 2
            ;;
        "-I" | "--grasp") 
            SCRID=$2
            shift 2
            ;;    
        "--help" | "-h")
            echo -e $usage
            exit 0
            ;;
        *)
            echo "[sprof] WARNING: invalid option '$1'. Ignored"
            shift 1
            ;;
    esac
done

#---------------------------------------------------------------------------
# Check scrutin id is set - and set the command for step 0b accordingly
#---------------------------------------------------------------------------
if [ -z "${SCRID}" ] ; then
    CMD0B="SELECT FLUSH_DATA_COLLECTOR();" 
    
  #---------------------------------------------------------------------------
  # Check user has dbadmin role
  #---------------------------------------------------------------------------
    if [ $(vsql -XAtqn -c "SELECT HAS_ROLE('dbadmin')") == 'f' ] ; then
        echo "User has no dbadmin role"
        exit 1
    fi
    
else
    CMD0B="ALTER SESSION SET UDPARAMETER SCRUTIN_ID=${SCRID};" 
    VMON=grasp  # Vertica monitor  schema
    VINT=grasp  # Vertica internal schema
    VCAT=grasp  # Vertica catalog schema
    NOTGRASP="-- "
fi 



#------------------------------------------------------------------------
# Start of sprof
#------------------------------------------------------------------------
secs=`date +%s`
echo "[`date +'%Y-%m-%d %H:%M:%S'`] ${SPV} started"


VVERSION=`vsql -Xtc "select version();" | sed -n 's/.* v\([0-9.]\+\)\..*/\1/p'`
if [[ "$(printf '%s\n' "$VVERSION" "$MIN_VVERSION" | sort -V | head -n 1)" == "$VVERSION" ]]; then
  
  secs=$(( `date +%s` - secs ))
  hh=$(( secs / 3600 ))
  mm=$(( ( secs / 60 ) % 60 ))
  ss=$(( secs % 60 ))
  printf "[%s] ${SPV} completed in %d sec (%02d:%02d:%02d)\n" "`date +'%Y-%m-%d %H:%M:%S'`" ${secs} $hh $mm $ss
  
  printf "\n\n**** ERORR **** \n${SPV} requires Vertica >= 9.3 to run properly, your version: [%s]\n Exiting..." ${VVERSION}
  exit 1
fi

#print date ranges
echo "Dates range: ${SDATE} to ${EDATE}. Output to ${OUT}"
#---------------------------------------------------------------------------
# Running system profile analysis
#---------------------------------------------------------------------------
cat <<-EOF | vsql -Xqn -P null='NULL' -o ${OUT} -f -
    \qecho ###------------------------------------------
    \qecho ### ${SPV}
    \qecho ### catalog schema: ${VCAT}
    \qecho ### monitor schema: ${VMON}
    \qecho ### internal schema: ${VINT}
    \qecho ### output file: ${OUT}
    \qecho ### gzip flag: ${GZP}
    \qecho ### start date: ${SDATE}
    \qecho ### end date: ${EDATE}
    \qecho ###------------------------------------------
    \qecho

    \set sdate '''${SDATE}''::TIMESTAMP'
    \set edate '''${EDATE}''::TIMESTAMP'

    -- ------------------------------------------------------------------------
    -- Start
    -- ------------------------------------------------------------------------
    \echo '    Step 0a: Script start timestamp'
    \qecho >>> Step 0a: Script start timestamp
    SELECT SYSDATE() AS 'Start Timestamp' ;

    \echo '    Step 0b: Flushing Data Collector /setting scrutin id'
    \qecho >>> Step 0b: Flushing Data Collector /setting scrutin id
    ${CMD0B}

    -- ------------------------------------------------------------------------
    -- System Information
    -- ------------------------------------------------------------------------
    \echo '    Step 1a: Vertica version'
    \qecho >>> Step 1a: Vertica version
    SELECT
        VERSION() 
    ;

    \pset expanded
    \echo '    Step 1b: System Information'
    \qecho >>> Step 1b: System Information
    SELECT * FROM ${VMON}.system ;
    \pset expanded
    
    
     
    \echo '    Step 1c: System Information (Deployment mode)'
    \qecho >>> Step 1c: System Information (Deployment mode)
     SELECT CASE COUNT(*)
         WHEN 0 THEN 'Enterprise'
         ELSE 'Eon'
       END AS "Database mode"
     FROM ${VCAT}.shards;
    

    -- ------------------------------------------------------------------------
    -- Get Data Collector Policy
    -- ------------------------------------------------------------------------
    \echo '    Step 2: Data Collector Policy'
    \qecho >>> Step 2: Data Collector Policy
    SELECT
        GET_DATA_COLLECTOR_POLICY('RequestsIssued');
    ;

    -- ------------------------------------------------------------------------
    -- Get Data Collector Details (Extended)
    -- ------------------------------------------------------------------------
    \echo '    Step 2b: Data Collector Policy - additional data'
    \qecho >>> Step 2b: Data Collector Policy - additional data
    SELECT
    	component
	   , (disk_size_kb / 1024)::NUMERIC(10,3) AS set_disk_size_MB
	   , (current_disk_bytes /(1024 ^2))::NUMERIC(10, 3) cur_dsk_util_MB
     , interval_set
	   , interval_time
	   , CAST(( CURRENT_TIMESTAMP - first_time) AS INTERVAL DAY TO MINUTE) AS 'intvl (DD HH:MM)'
    FROM
	  ${VMON}.data_collector
    WHERE
	    lower(table_name) IN ( 'columns', 'license_audits', 'projection_checkpoint_epochs', 'projection_columns', 'projections', 'resource_pools', 'tables', 'dc_cpu_aggregate_by_hour', 
	    'dc_execution_summaries', 'dc_lock_attempts', 'dc_memory_info_by_hour', 'dc_process_info_by_hour', 'dc_requests_completed', 'dc_requests_issued', 'dc_resource_acquisitions', 
	    'dc_slow_events', 'dc_spread_monitor', 'dc_tuple_mover_events', 'vs_elastic_cluster', 'delete_vectors', 'host_resources', 'projection_storage', 'query_profiles', 'query_requests', 
	    'resource_pool_status', 'storage_containers', 'system', 'dc_requests_issued', 'dc_requests_completed' )
      LIMIT 1 OVER (	PARTITION BY component ORDER BY node_name );
    ;

    -- ------------------------------------------------------------------------
    -- Getting Vertica non-default configuration parameters
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 3: Vertica non-default configuration parameters'
    \qecho >>> Step 3: Vertica non-default configuration parameters
    SELECT
        parameter_name, 
        current_value, 
        default_value, 
        description
    FROM 
        configuration_parameters 
    WHERE
        current_value <> default_value
    ORDER BY 
        parameter_name;
    ;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Resource Pools configuration
    -- ------------------------------------------------------------------------
    \echo '    Step 4: Resource Pools configuration'
    \qecho >>> Step 4: Resource Pools configuration
    SELECT
        rp.name,
        rp.memorysize AS MS,
        rp.maxmemorysize AS MMS,
        rp.maxquerymemorysize AS MQMS,
        rp.executionparallelism AS EP,
        rp.plannedconcurrency AS PC,
        rp.maxconcurrency as MC,
        rp.queuetimeout AS QT,
        rp.priority AS PRI,
        rp.runtimepriority AS RTPRI,
        rp.runtimeprioritythreshold AS RTPSH,
        rp.singleinitiator AS SI,
        rp.cpuaffinityset AS CPUA,
        rp.cpuaffinitymode AS CPUM,
        rp.runtimecap AS RTCAP,
        rp.cascadeto AS CASCADE,
        rs.QBMB
    FROM
        ${VCAT}.resource_pools rp
        INNER JOIN (
            SELECT pool_name, MIN(query_budget_kb)//1024 AS QBMB
            FROM ${VMON}.resource_pool_status
            GROUP BY pool_name) rs
            ON rp.name = rs.pool_name
    ORDER BY
        rp.is_internal, rp.name
    ;

    -- ------------------------------------------------------------------------
    -- Cluster Analysis
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 5a: Getting Cluster configuration'
    \qecho >>> Step 5a: Getting Cluster configuration
    SELECT
        *
    FROM
        ${VMON}.host_resources
    ;
    \pset expanded
    \echo '    Step 5b: Getting Elastic Cluster configuration'
    \qecho >>> Step 5b: Getting Elastic Cluster configuration
    SELECT
        version, 
        skew_percent, 
        scaling_factor, 
        is_enabled, 
        is_local_segment_enabled
    FROM 
        ${VINT}.vs_elastic_cluster
    ;

    \echo '    Step 5c: Spread Retransmit'
    \qecho >>> Step 5c: Spread Retransmit

    SELECT
        a."time" ,
        a.node_name ,
        a.retrans ,
        a.time_interval ,
        a.packet_count ,
        ((a.retrans / (a.time_interval / '00:00:01'::INTERVAL)))::NUMERIC(18,2) AS retrans_per_second
    FROM (
        SELECT
            (dc_spread_monitor."time")::timestamp AS "time" ,
            dc_spread_monitor.node_name ,
            (dc_spread_monitor.retrans -
             lag(dc_spread_monitor.retrans, 1, NULL::INT) OVER(
                PARTITION BY dc_spread_monitor.node_name 
                ORDER BY (dc_spread_monitor."time")::TIMESTAMP)) AS retrans,
            (((dc_spread_monitor."time")::TIMESTAMP -
               lag((dc_spread_monitor."time")::TIMESTAMP, 1, NULL::TIMESTAMP) OVER(
                PARTITION BY dc_spread_monitor.node_name
                ORDER BY (dc_spread_monitor."time")::TIMESTAMP))) AS time_interval,
            (dc_spread_monitor.packet_sent -
             lag(dc_spread_monitor.packet_sent, 1, NULL::INT) OVER(
                PARTITION BY dc_spread_monitor.node_name
                ORDER BY (dc_spread_monitor."time")::TIMESTAMP)) AS packet_count
        FROM
            ${VINT}.dc_spread_monitor
    ) a
    WHERE a.time BETWEEN :sdate AND :edate
    ORDER BY 1, 2
    ;
           
    \echo '    Step 5d: Things slower than expected'
    \qecho >>> Step 5d: Things slower than expected
    SELECT
        event_description ,
        count(*) ,
        MAX(threshold_us/1000)::INT As Max_threshold_ms ,
        MAX(duration_us /1000)::INT As Max_duration_ms
    FROM
        ${VINT}.dc_slow_events
    GROUP BY 1
    ORDER BY COUNT(*) DESC LIMIT 5
    ;

    -- ------------------------------------------------------------------------
    -- Database Size (raw & compressed)
    -- ------------------------------------------------------------------------
    \echo '    Step 6a: Database Size (raw)'
    \qecho >>> Step 6a: Database Size (raw)
    SELECT
        GET_COMPLIANCE_STATUS()
    ;

    \echo '    Step 6b: Database Size (compressed by schema)'
    \qecho >>> Step 6b: Database Size (compressed by schema)
    SELECT
        anchor_table_schema,
        COUNT(DISTINCT(anchor_table_name)) AS num_tables,
        COUNT(DISTINCT(projection_name)) AS num_projs,
        COUNT(*) AS num_psegs,
        (SUM(used_bytes)/(1024^3))::INT AS used_gib 
    FROM
        ${VMON}.projection_storage
    GROUP BY ROLLUP(anchor_table_schema)
    ORDER BY 
        4 desc
    ;
    
    \echo '    Step 6c: Database Size (license distribution)'
    \qecho >>> Step 6c: Database Size (license distribution)
     SELECT * FROM (
        SELECT 
           (license_size_bytes / 1024 ^3)::NUMERIC(10, 2) AS license_size_GB
          , (database_size_bytes / 1024 ^3)::NUMERIC(10, 2) AS database_size_GB
          , (usage_percent * 100)::NUMERIC(3, 1) AS usage_percent
          , audit_end_timestamp
          , license_name AS license_scope
      FROM ${VINT}.vs_license_audits
      LIMIT 1 OVER (PARTITION BY license_name ORDER BY audit_end_timestamp DESC)) lic
     ORDER BY license_scope ;



 -- MF: Do we really need this?     
    \echo '    Step 6d: License Usage to determine compression'
    \qecho >>> Step 6d: License Usage to determine compression


SELECT /*+label(CompressionRatio)*/ deployment_mode
    , license_name
    , audit_start_timestamp
    , CAST(database_size_bytes/1024^3 AS DECIMAL(14, 2)) AS raw_size_GB
    , CAST(compressed_size_per_storage/1024^3 AS DECIMAL(14, 2)) AS compressed_size_per_storage_GB
    , CAST(database_size_bytes / GREATEST(compressed_size_per_storage,1) AS DECIMAL(34, 2)) AS ratio_per_storage
    , CAST(compressed_size_per_data/1024^3 AS DECIMAL(14, 2)) as compressed_size_per_data_GB
    , CAST (database_size_bytes / GREATEST(compressed_size_per_data,1) AS DECIMAL(34, 2)) ratio_per_data
FROM
    (
        SELECT
            license_name    
            ,database_size_bytes
            , audit_start_timestamp
        FROM
            ${VINT}.vs_license_audits
        WHERE
            license_name <> 'Total' 
        LIMIT 1 OVER (PARTITION BY license_name ORDER BY audit_start_timestamp DESC )
        ) AS A
    , (
    SELECT
          floor(sum(compressed_size_with_HA)) AS compressed_size_per_storage
        , floor(sum(compressed_size_no_HA)) AS compressed_size_per_data
        , table_type
        , deployment_mode
    FROM
        (
        SELECT
             compressed_size_with_HA
             ,(SELECT CASE COUNT(*) WHEN 0 THEN 'Enterprise' ELSE 'Eon' END FROM ${VCAT}.shards) AS deployment_mode
             , (SELECT current_fault_tolerance FROM SYSTEM) AS ksafety 
             ,COALESCE( 
                CASE
                    WHEN is_segmented = TRUE AND  deployment_mode = 'Enterprise' THEN compressed_size_with_HA / (ksafety+1)
                    WHEN is_segmented = TRUE AND  deployment_mode = 'Eon' THEN compressed_size_with_HA
                    WHEN is_segmented = FALSE AND deployment_mode = 'Enterprise' THEN compressed_size_with_HA / (SELECT GREATEST(count(*),1) AS nodenum FROM nodes)
                    WHEN is_segmented = FALSE AND deployment_mode = 'Eon' THEN compressed_size_with_HA  --- replicas
                END, 0) AS compressed_size_no_HA
            , table_type
        FROM
            (
            SELECT
                  table_type
                , tname
                , is_segmented
                , sum(used_bytes) AS compressed_size_with_HA
            FROM
                (
                  SELECT
                      projection_storage.node_name
                    , tables.name AS tname
                    , CASE
                        WHEN (tables.flextable_format <> '') THEN 'Flex'
                        WHEN (tables.sourcestatement <> '') THEN 'External'
                        ELSE 'Regular'
                        END AS table_type
                    , COALESCE (used_bytes,0) AS used_bytes
                    , projections.is_segmented
                    , tables.sourcestatement
                    , projections.projection_name
                    FROM ${VINT}.vs_tables AS tables
                    LEFT JOIN ${VCAT}.projections ON
                        projections.anchor_table_id = tables.oid
                    LEFT JOIN ${VMON}.projection_storage  ON
                        projection_storage.projection_id = projections.projection_id
                    ) t
            GROUP BY
                t.table_type
                , tname
                , is_segmented
            ORDER BY
                t.table_type
                , tname
                , is_segmented ) p1
        ORDER BY
            table_type 
        ) p2
    GROUP BY
        table_type, deployment_mode ) AS B
    WHERE
    A.license_name = B.table_type;



    -- ------------------------------------------------------------------------
    -- Catalog analysis (7a-7e from Eugenia's analyze_catalog)
    -- ------------------------------------------------------------------------
    \echo '    Step 7a: Catalog Analysis (Column types)'
    \qecho >>> Step 7a: Catalog Analysis (Column types)
    SELECT
        data_type_id, 
        MAX(UPPER(split_part(data_type,'(',1))) AS  data_type, 
        COUNT(*) AS num_colums, 
        COUNT(DISTINCT table_id) AS num_tables,  
        MAX(data_type_length) AS max_length, 
        AVG(data_type_length)::INT AS avg_length  
    FROM 
        ${VCAT}.columns GROUP BY 1 ORDER BY 1;

    \echo '    Step 7b: Catalog Analysis (Top 30 Largest Schemas)'
    \qecho >>> Step 7b: Catalog Analysis (Top 30 Largest Schemas)
    SELECT
        anchor_table_schema, 
        COUNT(DISTINCT(anchor_table_name)) AS table_count,
        SUM(used_bytes)//1024^3 AS gb_size, 
        SUM(row_count)//1000 AS krows 
    FROM 
        ${VMON}.projection_storage 
    GROUP BY 1
    ORDER BY 2 desc
    LIMIT 30;

    \echo '    Step 7c: Catalog Analysis (Top 30 Tables with more columns)'
    \qecho >>> Step 7c: Catalog Analysis (Top 30 Tables with more columns)
    SELECT
        table_schema, 
        table_name, 
        COUNT(DISTINCT column_id) AS '#cols', 
        SUM(data_type_length) AS sum_col_length
    FROM 
        ${VCAT}.columns 
    GROUP BY 1,2 
    ORDER BY 3 desc 
    LIMIT 30;

    \echo '    Step 7d: Catalog Analysis (Top 30 Tables with largest rows)'
    \qecho >>> Step 7d: Catalog Analysis (Top 30 Tables with largest rows)
    SELECT
        table_schema, 
        table_name, 
        COUNT(DISTINCT column_id) AS '#cols', 
        SUM(data_type_length) AS sum_col_length
    FROM 
        ${VCAT}.columns 
    GROUP BY 1,2 
    ORDER BY 4 desc 
    LIMIT 30;

    \echo '    Step 7e: Catalog Analysis (Top 30 largest segmented projections)'
    \qecho >>> Step 7e: Catalog Analysis (Top 30 largest segmented projections)
        SELECT
        ps.anchor_table_schema || '.' ||  ps.anchor_table_name ||
            '.' || ps.projection_name AS 'schema.table.projection' ,
        SUM(ps.row_count) AS row_count, 
        SUM(ps.used_bytes)//1024^2 AS used_mbytes,
        (CASE WHEN MIN(ps.row_count) = 0 THEN -1000.00
              ELSE MAX(ps.row_count)/MIN(ps.row_count) END)::NUMERIC(8,2) AS skew_ratio,
        -- SUM(ps.wos_row_count) AS wos_rows, 
		-- (CASE WHEN REGEXP_SUBSTR (version()::varchar, '.+v(\d+).\d+.\d+-\d+',1,1,'',1)::INT >= 10  THEN -1 ELSE SUM(ps.wos_row_count) END) AS wos_rows, 
		-- SUM(ps.wos_used_bytes)//1024^2 AS wos_mbytes, 
		-- (CASE WHEN REGEXP_SUBSTR (version()::varchar, '.+v(\d+).\d+.\d+-\d+',1,1,'',1)::INT >= 10  THEN -1 ELSE SUM(ps.wos_used_bytes)//1024^2 END) AS wos_mbytes, 
		--SUM(ps.ros_row_count) AS ros_rows, 
		SUM(ps.row_count) AS ros_rows, 
        --SUM(ps.ros_used_bytes)//1024^2 AS ros_mbytes,
		SUM(ps.used_bytes)//1024^2 AS ros_mbytes,									   
        COUNT(sc.projection_id) AS ros_count,
        SUM(sc.deleted_row_count) AS del_rows,
        SUM(sc.delete_vector_count) AS DVC,
        MIN(pce.checkpoint_epoch) AS CPE, 
        MIN(pce.is_up_to_date) AS UTD 
    FROM
        ${VMON}.projection_storage ps
        INNER JOIN ${VCAT}.projections p USING (projection_id)
        INNER JOIN ${VMON}.storage_containers sc USING(projection_id)
        INNER JOIN ${VCAT}.projection_checkpoint_epochs pce USING(projection_id)
    WHERE p.is_segmented AND ps.projection_name LIKE '%_b0'
    GROUP BY 1
    ORDER BY 3 desc
    LIMIT 30
    ;

    \echo '    Step 7f: Catalog Analysis (Top 30 largest unsegmented projections)'
    \qecho >>> Step 7f: Catalog Analysis (Top 30 largest unsegmented projections)
    SELECT
        ps.anchor_table_schema || '.' ||  ps.anchor_table_name ||
            '.' || p.projection_name AS 'schema.table.proj' ,
        SUM(ps.row_count) AS row_count, 
        SUM(ps.used_bytes)//1024^2 AS used_bytes,
        -- SUM(ps.wos_row_count) AS wos_rows, 
		-- (CASE WHEN REGEXP_SUBSTR (version()::varchar, '.+v(\d+).\d+.\d+-\d+',1,1,'',1)::INT >= 10  THEN -1 ELSE SUM(ps.wos_row_count) END) AS wos_rows, 
		-- SUM(ps.wos_used_bytes)//1024^2 AS wos_mbytes, 
		-- (CASE WHEN REGEXP_SUBSTR (version()::varchar, '.+v(\d+).\d+.\d+-\d+',1,1,'',1)::INT >= 10  THEN -1 ELSE SUM(ps.wos_used_bytes)//1024^2 END) AS wos_mbytes, 
		--SUM(ps.ros_row_count) AS ros_rows, 
		SUM(ps.row_count) AS ros_rows, 
        --SUM(ps.ros_used_bytes)//1024^2 AS ros_mbytes,
		SUM(ps.used_bytes)//1024^2 AS ros_mbytes,								   
        COUNT(sc.projection_id) AS ros_count,
        SUM(sc.deleted_row_count) AS del_rows,
        SUM(sc.delete_vector_count) AS DVC,
        MIN(pce.checkpoint_epoch) CPE, 
        MIN(pce.is_up_to_date) UTD 
    FROM
        ${VMON}.projection_storage ps
        INNER JOIN ${VCAT}.projections p USING (projection_id)
        INNER JOIN ${VMON}.storage_containers sc USING(projection_id)
        INNER JOIN ${VCAT}.projection_checkpoint_epochs pce USING(projection_id)
    WHERE NOT is_segmented
    GROUP BY 1
    ORDER BY 3 desc
    LIMIT 30
    ;

    \echo '    Step 7g: Catalog Analysis (Top 30 most used projections)'
    \qecho >>> Step 7g: Catalog Analysis (Top 30 most used projections)
    SELECT
        pu.anchor_table_schema || '.' ||  pu.anchor_table_name ||
            '.' || pu.projection_name AS 'schema.table.projection' ,
        COUNT(*) AS num_queries
    FROM
        ${VMON}.projection_usage pu
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 30
    ;

    \echo '    Step 7h: Catalog Analysis (Top 30 less used projections)'
    \qecho >>> Step 7h: Catalog Analysis (Top 30 less used projections)
    SELECT
        pu.anchor_table_schema || '.' ||  pu.anchor_table_name ||
            '.' || pu.projection_name AS 'schema.table.projection' ,
        COUNT(*) AS num_queries
    FROM
        ${VMON}.projection_usage pu
    GROUP BY 1
    ORDER BY 2
    LIMIT 30
    ;

    \echo '    Step 7i: Catalog Analysis (Tables per schema)'
    \qecho >>> Step 7i: Catalog Analysis (Tables per schema)
    SELECT
        CASE WHEN table_schema IS NULL THEN 'Grand Total' ELSE table_schema END AS Schema,  
        SUM(CASE WHEN LENGTH(partition_expression) > 0 THEN 1 ELSE 0 END) AS 'Partitioned',
        SUM(CASE WHEN LENGTH(partition_expression) = 0 THEN 1 ELSE 0 END) AS 'Not Partitioned',
        COUNT(*) AS 'Total'
    FROM 
        ${VCAT}.tables 
    GROUP BY ROLLUP(table_schema)
    ORDER BY GROUPING(table_schema)
    ;

    \echo '    Step 7j: Catalog Analysis (Catalog Size)'
    \qecho >>> Step 7j: Catalog Analysis (Catalog Size)
    SELECT
        DATE_TRUNC('day',ts) AS 'date', 
        node_name, 
        MAX(catalog_size_in_MB)::INT AS END_CATLOG_SIZE_MEM_MB
    FROM ( SELECT
            node_name, 
            TRUNC((dc_allocation_pool_statistics_by_second."time")::TIMESTAMP,'SS'::VARCHAR(2)) AS ts, 
            SUM((dc_allocation_pool_statistics_by_second.total_memory_max_value -
                 dc_allocation_pool_statistics_by_second.  free_memory_min_value))/ (1024*1024) AS catalog_size_in_MB
           FROM 
            ${VINT}.dc_allocation_pool_statistics_by_second 
           GROUP BY 1, TRUNC((dc_allocation_pool_statistics_by_second."time")::TIMESTAMP,'SS'::VARCHAR(2)) ) foo
    GROUP BY 1,2 
    ORDER BY 1 DESC, 2;

    \echo '    Step 7k: Catalog Analysis (Number of tables)'
    \qecho >>> Step 7k: Catalog Analysis (Number of tables)
    SELECT COUNT(*) AS num_tables FROM ${VCAT}.tables ;

    \echo '    Step 7l: Catalog Analysis (Number of projections)'
    \qecho >>> Step 7l: Catalog Analysis (Number of projections)
    SELECT COUNT(*) AS num_projections FROM ${VCAT}.projections ;

    \echo '    Step 7m: Catalog Analysis (Number of projection basenames by table)'
    \qecho >>> Step 7m: Catalog Analysis (Number of projection basenames by table)
    SELECT
        n, count(*)
    FROM
        (SELECT
             anchor_table_name,
             COUNT(DISTINCT projection_basename) AS n
         FROM ${VCAT}.projections
         GROUP BY 1) x
    GROUP BY 1
    ORDER BY 2 DESC;

    \echo '    Step 7n: Catalog Analysis (Number of columns)'
    \qecho >>> Step 7n: Catalog Analysis (Number of columns)
    SELECT COUNT(*) AS num_columns FROM ${VCAT}.columns ;

    \echo '    Step 7o: Catalog Analysis (Number of delete vectors)'
    \qecho >>> Step 7o: Catalog Analysis (Number of delete vectors)
   SELECT node_name, 
		   -- CASE WHEN REGEXP_SUBSTR (version()::varchar, '.+v(\d+).\d+.\d+-\d+',1,1,'',1)::INT >= 10  THEN 'ROS' ELSE storage_type END)  as storage_type, 
		   'ROS' as storage_type,					   
           COUNT(*) AS num_del_vectors,
           SUM(deleted_row_count) AS num_del_rows,
           SUM(used_bytes) AS used_byted
    FROM ${VMON}.delete_vectors
    GROUP BY 1, 2
    ORDER BY 1;

    \echo '    Step 7p: Catalog Analysis (Columns by statistics)'
    \qecho >>> Step 7p: Catalog Analysis (Columns by statistics)
    SELECT statistics_type, COUNT(*) AS count FROM ${VCAT}.projection_columns GROUP BY 1 ;

    \echo '    Step 7q: Catalog Analysis (Data types by encoding)'
    \qecho >>> Step 7q: Catalog Analysis (Data types by encoding)
    SELECT 
        UPPER(SPLIT_PART(data_type,'(',1)) AS data_type, 
        CASE WHEN sort_position > 0 THEN true ELSE false END AS is_sorted, 
        encoding_type, 
        COUNT(*) AS count
    FROM ${VCAT}.projection_columns
    GROUP BY 1, 2, 3 
    ORDER BY 1, 2;

    \echo '    Step 7r: Catalog Analysis (Number of storage containers)'
    \qecho >>> Step 7r: Catalog Analysis (Number of storage containers)
    SELECT node_name, COUNT(*) AS num_storage_containers
    FROM ${VMON}.storage_containers GROUP BY 1;

    \echo '    Step 7s: Catalog Analysis (Distinct Projection basenames by Creation Type)'
    \qecho >>> Step 7s: Catalog Analysis (Distinct Projection basenames by Creation Type)
    SELECT
        create_type,
        COUNT(DISTINCT projection_basename) AS distinct_basenames
    FROM
        ${VCAT}.projections
    GROUP BY 1
    ORDER BY 2 DESC;

    -- ------------------------------------------------------------------------
    -- High-level workload definition by hour & query type
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 8a: Query_requests history analysis'
    \qecho >>> Step 8a: Query_requests history analysis
    SELECT
        MIN(start_timestamp) AS min_timestamp, 
        MAX(start_timestamp) AS max_timestamp, 
        COUNT(*) AS num_queries
    FROM 
        ${VMON}.query_requests 
    WHERE 
        is_executing is false AND
        start_timestamp BETWEEN :sdate AND :edate
    ;
    \pset expanded

    \echo '    Step 8b: Throughput by hour and Q type'
    \qecho >>> Step 8b: Throughput by hour and Q type
    SELECT
        TIME_SLICE(qr.start_timestamp, 1, 'hour','start') AS time_slice,
        CASE WHEN UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
             ELSE UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) END AS qtype,
        qr.request_type ,
        COUNT(*) AS count,
        MIN(qr.request_duration_ms) AS min_ms,
        MAX(qr.request_duration_ms) AS max_ms,
        AVG(qr.request_duration_ms)::INT AS avg_ms,
        MIN(qr.memory_acquired_mb) AS min_mb,
        MAX(qr.memory_acquired_mb) AS max_mb,
        AVG(qr.memory_acquired_mb)::INT AS avg_mb
    FROM
        ${VMON}.query_requests qr
    WHERE
        qr.is_executing IS false AND
        start_timestamp BETWEEN :sdate AND :edate
    GROUP BY 
        1, 2, 3
    ORDER BY 
        1, 2, 3
    ;

    \echo '    Step 8c: Query Consumption by Resource Pool'
    \qecho >>> Step 8c: Query Consumption by Resource Pool
    WITH qc AS ( 
        SELECT
            es.cpu_cycles_us, es.network_bytes_received, es.network_bytes_sent, 
            es.data_bytes_read, es.data_bytes_written, es.data_bytes_loaded, 
            es.bytes_spilled, es.input_rows, es.input_rows_processed, es.thread_count, 
            datediff('millisecond', ri.time, rc.time) as duration_ms, 
            case when count_rp=1 then es.resource_pool else '<multiple>' end as resource_pool, 
            output_rows, request_type, success 
        FROM (
            SELECT transaction_id, statement_id, 
                   sum(cpu_time_us) as cpu_cycles_us, sum(network_bytes_received) as network_bytes_received, 
                   sum(network_bytes_sent) as network_bytes_sent, sum(data_bytes_read) as data_bytes_read, 
                   sum(data_bytes_written) as data_bytes_written, sum(data_bytes_loaded) as data_bytes_loaded, sum(bytes_spilled) as bytes_spilled, 
                   sum(input_rows) as input_rows, sum(input_rows_processed) as input_rows_processed, 
                   sum(thread_count) as thread_count, max(res_pool) as resource_pool, count(distinct res_pool) as count_rp 
            FROM ${VINT}.dc_execution_summaries 
            WHERE time BETWEEN :sdate AND :edate
            GROUP BY transaction_id, statement_id                                                                       
            ) es 
            LEFT OUTER JOIN 
            ( 
            SELECT transaction_id, statement_id, 
                   time, request_type, label, is_retry 
            FROM ${VINT}.dc_requests_issued 
            ) ri 
            USING (transaction_id, statement_id) 
            LEFT OUTER JOIN 
            ( 
            SELECT transaction_id, statement_id, 
                   time, processed_row_count as output_rows, success 
            FROM ${VINT}.dc_requests_completed 
            ) rc 
            USING (transaction_id, statement_id)
    )
    SELECT
        resource_pool,
        COUNT(*) AS count,
        SUM(cpu_cycles_us)//1000000 AS tot_cpu_s,
        SUM(network_bytes_received)//(1024*1024) AS net_mb_in,
        SUM(network_bytes_sent)//(1024*1024) AS net_mb_out,
        SUM(data_bytes_read)//(1024*1024) AS mbytes_read,
        SUM(data_bytes_written)//(1024*1024) AS mbytes_written,
        SUM(data_bytes_loaded)//(1024*1024) AS mbytes_loaded,
        SUM(bytes_spilled)//(1024*1024) AS mbytes_spilled,
        SUM(input_rows)//1000000 AS mrows_in,
        SUM(input_rows_processed)//1000000 AS mrows_proc,
        SUM(output_rows)//1000000 AS mrows_out,
        SUM(thread_count) AS tot_thread_count,
        SUM(duration_ms)//1000 AS tot_duration_s
    FROM
        qc
    WHERE
        request_type = 'QUERY'
    GROUP BY 
        1
    ORDER BY 
        1
    ;

    \echo '    Step 8d: Query Events Analysis by Request Type'
    \qecho >>> Step 8d: Query Events Analysis by Request Type
    SELECT
        qr.request_type,
        qe.event_category,
        qe.event_type,
        COUNT(*)
    FROM ${VMON}.query_requests qr
        LEFT JOIN ${VMON}.query_events qe
        USING( transaction_id, STATEMENT_ID)
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 4 DESC;

    \echo '    Step 8e: Query Events Analysis by Statement Type'
    \qecho >>> Step 8e: Query Events Analysis by Statement Type
    SELECT
        UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) AS request,
        qe.event_category,
        qe.event_type,
        COUNT(*)
    FROM ${VMON}.query_requests qr
        LEFT JOIN ${VMON}.query_events qe
        USING( transaction_id, STATEMENT_ID)
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 4 DESC;

    -- ------------------------------------------------------------------------
    -- Query Elapsed distribution
    -- ------------------------------------------------------------------------
    \echo '    Step 9a: Query Elapsed distribution overview'
    \qecho >>> Step 9a: Query Elapsed distribution overview
    SELECT
        ra.pool_name,
        qr.request_type,
         CASE WHEN UPPER(LEFT(request||';', 9)::CHAR(9)) = 'SELECT 1;' OR UPPER(LEFT(request||';', 19)::CHAR(19)) = 'SELECT 1 FROM DUAL;' THEN 'HEARTBEAT'
         WHEN UPPER(REGEXP_SUBSTR(LTRIM(request),'\w+')::CHAR(8)) = 'SELECT' AND ( NOT request ~~*'%from%' OR request ~~*'%from dual;') THEN 'EXPRESSION'
         WHEN COALESCE(UPPER(TRIM(REGEXP_SUBSTR(request,'(?<=from|FROM)(\s+\w+\.?\w*\b)'))::CHAR(256)),'foo') IN ( SELECT UPPER(table_schema||'.'||table_name) AS tab FROM system_tables vst UNION ALL SELECT UPPER(table_name) FROM system_tables vst) THEN 'SYSQUERY'
         WHEN UPPER(REGEXP_SUBSTR(request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
         ELSE UPPER(REGEXP_SUBSTR(request, '\w+', 1, 1, 'b')::CHAR(8))
        END AS qtype,
        SUM(CASE WHEN qr.request_duration_ms < 1000 then 1 else 0 end) AS 'less1s',
        SUM(CASE WHEN qr.request_duration_ms >= 1000 AND qr.request_duration_ms < 2000 THEN 1 ELSE 0 END) AS '1to2s',
        SUM(CASE WHEN qr.request_duration_ms >= 2000 AND qr.request_duration_ms < 5000 THEN 1 ELSE 0 END) AS '2to5s',
        SUM(CASE WHEN qr.request_duration_ms >= 5000 AND qr.request_duration_ms < 10000 THEN 1 ELSE 0 END) AS '5to10s',
        SUM(CASE WHEN qr.request_duration_ms >= 10000 AND qr.request_duration_ms < 20000 THEN 1 ELSE 0 END) AS '10to20s',
        SUM(CASE WHEN qr.request_duration_ms >= 20000 AND qr.request_duration_ms < 60000 THEN 1 ELSE 0 END) AS '20to60s',
        SUM(CASE WHEN qr.request_duration_ms >= 60000 AND qr.request_duration_ms < 120000 THEN 1 ELSE 0 END) AS '1to2m',
        SUM(CASE WHEN qr.request_duration_ms >= 120000 AND qr.request_duration_ms < 300000 THEN 1 ELSE 0 END) AS '2to5m',
        SUM(CASE WHEN qr.request_duration_ms >= 300000 AND qr.request_duration_ms < 600000 THEN 1 ELSE 0 END) AS '5to10m',
        SUM(CASE WHEN qr.request_duration_ms >= 600000 AND qr.request_duration_ms < 6000000 THEN 1 ELSE 0 END) AS '10to60m',
        SUM(CASE WHEN qr.request_duration_ms >= 6000000 THEN 1 ELSE 0 END) AS 'more1h'
    FROM
        ${VMON}.query_requests qr
        INNER JOIN ${VINT}.dc_resource_acquisitions ra
        USING(transaction_id, statement_id)
    WHERE
        is_executing IS false AND
        start_timestamp BETWEEN :sdate AND :edate
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    ;

    \echo '    Step 9b: Detailed SELECT Elapsed distribution'
    \qecho >>> Step 9b: Detailed SELECT Elapsed distribution
    SELECT
        pool_name,
        bucket,
        COUNT(*) AS count
    FROM (
        SELECT
            ra.pool_name, 
            1 + qr.request_duration_ms // 1000 AS bucket
        FROM
            ${VMON}.query_requests qr
            INNER JOIN ${VINT}.dc_resource_acquisitions ra
            USING(transaction_id, statement_id)
        WHERE
            qr.request_type = 'QUERY' AND
            qr.start_timestamp BETWEEN :sdate AND :edate AND
            qr.request_duration_ms IS NOT NULL AND
            ( UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' OR
              UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'SELECT' )
        ) x
    GROUP BY 1, 2
    ORDER BY 1, 2
    ;
    
    \echo '    Step 9c: Statements Execution percentile (query duration in ms)'
    \qecho >>> Step 9c: Statements Execution percentile (query duration in ms)
    SELECT DISTINCT * FROM (
        SELECT
            pool_name,
            query_type,
            query_cat,
            COUNT(*) OVER (PARTITION BY pool_name, query_type, query_cat) AS count ,
            (percentile_disc(.1) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_10  ,
            (percentile_disc(.2) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_20 ,
            (percentile_disc(.3) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_30  ,
            (percentile_disc(.4) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_40  ,
            (percentile_disc(.5) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_50  ,
            (percentile_disc(.6) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_60  ,
            (percentile_disc(.7) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_70  ,
            (percentile_disc(.8) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_80  ,
            (percentile_disc(.9) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_90  ,
            (percentile_disc(1) WITHIN GROUP (ORDER BY query_duration_us )
                OVER  (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_100
        FROM (
            SELECT
                ra.pool_name,
                qp.query_type,
                CASE WHEN UPPER(LEFT(qp.query||';', 9)::CHAR(9)) = 'SELECT 1;' OR UPPER(LEFT(qp.query||';', 19)::CHAR(19)) = 'SELECT 1 FROM DUAL;' THEN 'HEARTBEAT'
         	           WHEN UPPER(REGEXP_SUBSTR(LTRIM(qp.query),'\w+')::CHAR(8)) = 'SELECT' AND ( NOT qp.query ~~*'%from%' OR qp.query ~~*'%from dual;') THEN 'EXPRESSION'
                     WHEN UPPER(REGEXP_SUBSTR(qp.query, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
                     WHEN COALESCE(UPPER(TRIM(REGEXP_SUBSTR(qp.query,'(?<=from|FROM)(\s+\w+\.?\w*\b)'))::CHAR(256)),'foo') IN ( SELECT UPPER(table_schema||'.'||table_name) AS tab FROM system_tables vst UNION ALL SELECT UPPER(table_name) FROM system_tables vst) THEN 'SYSQUERY'
                     ELSE UPPER(REGEXP_SUBSTR(qp.query, '\w+', 1, 1, 'b')::CHAR(8)) END AS query_cat,
                qp.query_duration_us
            FROM
                ${VMON}.query_profiles qp
                LEFT OUTER JOIN ${VINT}.dc_resource_acquisitions ra
                USING(transaction_id, statement_id)
            WHERE
                qp.query_start::TIMESTAMP BETWEEN :sdate AND :edate
        ) a
    ) b
    ORDER BY 1,2 
    ;

    \echo '    Step 9d: Statements Counts '
    \qecho >>> Step 9d: Statements Counts 
    SELECT
        ra.pool_name,
        qr.request_type,
         CASE WHEN UPPER(LEFT(request||';', 9)::CHAR(9)) = 'SELECT 1;' OR UPPER(LEFT(request||';', 19)::CHAR(19)) = 'SELECT 1 FROM DUAL;' THEN 'HEARTBEAT'
         WHEN UPPER(REGEXP_SUBSTR(LTRIM(request),'\w+')::CHAR(8)) = 'SELECT' AND ( NOT request ~~*'%from%' OR request ~~*'%from dual;') THEN 'EXPRESSION'
         WHEN COALESCE(UPPER(TRIM(REGEXP_SUBSTR(request,'(?<=from|FROM)(\s+\w+\.?\w*\b)'))::CHAR(256)),'foo') IN ( SELECT UPPER(table_schema||'.'||table_name) AS tab FROM system_tables vst UNION ALL SELECT UPPER(table_name) FROM system_tables vst) THEN 'SYSQUERY'
         WHEN UPPER(REGEXP_SUBSTR(request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
         ELSE UPPER(REGEXP_SUBSTR(request, '\w+', 1, 1, 'b')::CHAR(8))
        END AS qtype,
        COUNT(DISTINCT qr.node_name) AS nodes_data,
        COUNT(*) AS num_queries,
        MIN(qr.start_timestamp) AS Min_TS,
        MAX(qr.start_timestamp) AS Max_TS,
        SUM(qr.request_duration_ms) // 1000 As Total_Runtime_s,
        AVG(qr.request_duration_ms) // 1000 As AVG_Runtime_s,
        MIN(qr.request_duration_ms)//1000 As MIN_Runtime_s,
        MAX(qr.request_duration_ms)//1000 As MAX_Runtime_s
    FROM
        ${VMON}.query_requests qr
        INNER JOIN ${VINT}.dc_resource_acquisitions ra
        USING(transaction_id, statement_id)
    WHERE
        qr.success='t' AND
        start_timestamp BETWEEN :sdate AND :edate
    GROUP BY 1, 2, 3
    ORDER BY 1, 2
    ;
	

    -- ------------------------------------------------------------------------
    -- Concurrency Analysis
    -- ------------------------------------------------------------------------
    \echo '    Step 10: Query Concurrency'
    \qecho >>> Step 10: Query Concurrency
    SELECT
        qts::TIMESTAMP AS timestamp,
        request_type,
        SUM(qd) OVER (PARTITION BY request_type ORDER BY qts) concurrency
    FROM (
        SELECT  -- Each start contributes with +1
            request_type,
            start_timestamp AS qts, 
            1 AS qd
        FROM 
            ${VMON}.query_requests 
        WHERE
            end_timestamp IS NOT NULL AND
            start_timestamp BETWEEN :sdate AND :edate
        UNION ALL 
        SELECT  -- Each end contributes with -1
            request_type,
            end_timestamp AS qts, 
            -1 AS qd
        FROM
            ${VMON}.query_requests 
        WHERE
            end_timestamp IS NOT NULL AND
            start_timestamp BETWEEN :sdate AND :edate
    ) x
    ;

    \echo '    Step 10b: Resource Queue Wait '
    \qecho >>> Step 10b: Resource Queue Wait

	SELECT 
		pool_name as pool_name ,
		queuetimeout, count(*) as query_count,
		max(DATEDIFF('ms',start_time,time)) maximum_wait_in_msec,
		min(DATEDIFF('ms',start_time,time)) minimum_wait_in_msec,
		avg(DATEDIFF('ms',start_time,time))::int average_wait_in_msec,
		count(distinct day(time)) retention_history_days
	FROM
		${VINT}.dc_resource_acquisitions ,  ${VCAT}.resource_pools 
	WHERE 
		name = pool_name AND 
		request_type = 'Reserve' AND 
		DATEDIFF('ms',start_time,time) >= 100
	GROUP BY 1,2 
	ORDER BY  4 desc;

    -- ------------------------------------------------------------------------
    -- Epochs & Delete Vectors status
    -- ------------------------------------------------------------------------
    \echo '    Step 11a: Epoch Status'
    \qecho >>> Step 11a: Epoch Status
    SELECT 'Last Good Epoch' AS epoch, epoch_number, epoch_close_time
    FROM ${VMON}.system INNER JOIN ${VCAT}.epochs ON epoch_number = last_good_epoch 
    UNION ALL
    SELECT 'Ancient History Mark' AS epoch, epoch_number, epoch_close_time
    FROM ${VMON}.system INNER JOIN ${VCAT}.epochs ON epoch_number = ahm_epoch 
    UNION ALL
    SELECT 'Current Epoch' AS epoch, current_epoch AS epoch_number, epoch_close_time
    FROM ${VMON}.system LEFT OUTER JOIN ${VCAT}.epochs ON epoch_number = current_epoch 
    ;

    \echo '    Step 11b: Delete Vector Status'
    \qecho >>> Step 11b: Delete Vector Status
    SELECT
        start_epoch,
        end_epoch,
        -- CASE WHEN REGEXP_SUBSTR (version()::varchar, '.+v(\d+).\d+.\d+-\d+',1,1,'',1)::INT >= 10  THEN 'ROS' ELSE storage_type END)  as storage_type,
		'ROS' as storage_type,				
        SUM(deleted_row_count) AS sum_deleted_rows
    FROM 
        ${VMON}.delete_vectors 
    GROUP BY 1, 2, 3;

    -- ------------------------------------------------------------------------
    -- Lock Usage
    -- ------------------------------------------------------------------------
    \echo '    Step 12a: Lock Attempts Overview'
    \qecho >>> Step 12a: Lock Attempts Overview
    SELECT
        SPLIT_PART(object_name, ':', 1) AS object,
        mode,
        result,
        description,
        COUNT(*) AS num_locks
    FROM
        ${VINT}.dc_lock_attempts
    GROUP BY 1, 2, 3, 4
    ORDER BY 5 DESC
    ;

    \echo '    Step 12b: Lock Attempts by hour/type'
    \qecho >>> Step 12b: Lock Attempts by hour/type
    SELECT
        TIME_SLICE(start_time, 1, 'hour', 'start') AS time_slice,
        CASE WHEN object=0 AND mode='X' THEN 'GCL_X'
             WHEN object=0 AND mode='S' THEN 'GCL_S'
             WHEN object=1 AND mode='X' THEN 'LCL_X'
             WHEN object=1 AND mode='S' THEN 'LCL_S'
             ELSE SPLIT_PART(object_name, ':', 1) || '_' || mode 
             END AS lock_type,
        COUNT(*) AS count
    FROM
        ${VINT}.dc_lock_attempts
    WHERE
        start_time BETWEEN :sdate AND :edate
    GROUP BY 1,2
    ORDER BY 1,2
    ;

    -- ------------------------------------------------------------------------
    -- Hardware Resource Usage
    -- ------------------------------------------------------------------------
    \echo '    Step 13a: CPU by hour'
    \qecho >>> Step 13a: CPU by hour
    SELECT
        * 
    FROM
        ${VINT}.dc_cpu_aggregate_by_hour
    WHERE
        time BETWEEN :sdate AND :edate
    ORDER BY
        time;

    \echo '    Step 13b: Memory by hour'
    \qecho >>> Step 13b: Memory by hour
    SELECT
        time,
        node_name, 
        total_memory_start_value,
        free_memory_start_value,
        buffer_memory_start_value,
        file_cache_memory_start_value
    FROM
        ${VINT}.dc_memory_info_by_hour
    WHERE
        time BETWEEN :sdate AND :edate
    ORDER BY
        time;

    \echo '    Step 13c: Processs Info'
    \qecho >>> Step 13c: Processs Info
    SELECT
        node_name ,
        start_time ,
        process ,
        address_space_max ,
        data_size_max ,
        open_files_max ,
        threads_max ,
        files_open_max_value ,
        sockets_open_max_value ,
        other_open_max_value ,
        virtual_size_max_value ,
        resident_size_max_value ,
        shared_size_max_value ,
        text_size_max_value ,
        data_size_max_value ,
        library_size_max_value ,
        dirty_size_max_value ,
        thread_count_max_value ,
        map_count_max_value
    FROM
        ${VINT}.dc_process_info_by_hour 
    WHERE
        start_time BETWEEN :sdate AND :edate
    ORDER BY 1, 2;

    -- ------------------------------------------------------------------------
    --  TM events
    -- ------------------------------------------------------------------------
    \echo '    Step 14a: TM events'
    \qecho >>> Step 14a: TM events
    SELECT
        TIME_SLICE(time, 1, 'hour','start') AS time_slice ,
        node_name,
        operation,
        COUNT(*) AS count,
        SUM(container_count) AS containers,
        SUM(total_size_in_bytes)//1024//1024 AS size_mb
    FROM
        ${VINT}.dc_tuple_mover_events
    WHERE
        time BETWEEN :sdate AND :edate
    GROUP BY 1, 2, 3
    ORDER BY 1;

    \echo '    Step 14b: TM durations'
    \qecho >>> Step 14b: TM durations
    SELECT
        operation, 
        MIN(start_time) min_time, 
        MAX(start_time) max_time ,
        MAX(duration) ,
        MIN(duration) ,
        AVG(duration) ,
        COUNT(DISTINCT transaction_id) num_events
    FROM (
        SELECT
            start.TIME AS start_time ,
            complete.TIME AS complete_time ,
            complete.TIME - start.TIME AS duration ,
            start.node_name ,
            start.transaction_id ,
            start.operation ,
            start.event AS start_event ,
            complete.event AS complete_event
        FROM (
            SELECT * FROM ${VINT}.dc_tuple_mover_events WHERE event = 'Start') AS start
        INNER JOIN (
            SELECT * FROM ${VINT}.dc_tuple_mover_events WHERE event = 'Complete') AS complete
            USING (transaction_id, node_name)
        WHERE
            start.TIME BETWEEN :sdate AND :edate
        ORDER BY start.TIME
        ) sq
    GROUP BY 1;

    \echo '    Step 14c: Long Mergeout (> 20 mins)'
    \qecho >>> Step 14c: Long Mergeout (> 20 mins)
    SELECT
        a.node_name ,
        a.schema_name ,
        b.projection_name ,
        COUNT(*)
    FROM
        ${VINT}.dc_tuple_mover_events a 
        INNER JOIN
        ${VINT}.dc_tuple_mover_events b
        USING(transaction_id)
    WHERE
        a.event = 'Start' AND
        b.event = 'Complete' AND
        a.time BETWEEN :sdate AND :edate AND
        b.time::TIMESTAMP - a.time::TIMESTAMP > INTERVAL '20 minutes'
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
    ;

    \echo '    Step 14d: Long Running Reply Delete (> 10 mins)'
    \qecho >>> Step 14d: Long Running Reply Delete (> 10 mins)
    SELECT
        a.node_name ,
        a.schema_name ,
        b.projection_name ,
        COUNT(*)
    FROM
        ${VINT}.dc_tuple_mover_events a
        INNER JOIN
        ${VINT}.dc_tuple_mover_events b
        USING(transaction_id)
    WHERE
        a.event = 'Change plan type to Replay Delete' AND
        b.event = 'Complete' AND
        a.time BETWEEN :sdate AND :edate AND
        b.time - a.time > INTERVAL '10 minutes'
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
    ;
   \echo '    Step 15a: Additional Testing Queries - LockAttempts (VAdvisor format)'
   \qecho >>> Step 15a: Additional Testing Queries - LockAttempts (VAdvisor format)
	
	SELECT
		/*+label(lockReleassStatsByLockType)*/
		CASE
			WHEN substr( object_name, 1, 5) = 'Table' THEN 'Table'
			WHEN substr( object_name, 1, 10) = 'Projection' THEN 'Projection'
			WHEN object_name = '<Unknown or deleted object>' THEN '<Unknown or deleted object>'
			ELSE object_name
		END
		, mode
		, RESULT
		, count(*)
		, (count( CASE WHEN duration >= 1 THEN 1 ELSE NULL END )/ count(*)* 100 )::int wait_percent
		, approximate_percentile( duration USING parameters percentile = 0.90)::int AS percentile_90_wait
		, max(duration) max_wait 
		, CASE
			WHEN (
				CASE
					WHEN substr( object_name, 1, 5 ) = 'Table' THEN 'Table'
					WHEN substr( object_name, 1, 10 ) = 'Projection' THEN 'Projection'
					WHEN object_name = '<Unknown or deleted object>' THEN '<Unknown or deleted object>'
					ELSE object_name
				END = 'Global Catalog'
				AND mode = 'X'
				AND min(t.History_hours) < 24
			) THEN 'dc_lock_attempts holding less than ' || min(t.History_hours)+ 1 || ' hour(s) of historical data,increase retention by running select set_data_collector_policy(''LockAttempts'',''500'',''500000'');'
			WHEN
			CASE
				WHEN substr( object_name, 1, 5 ) = 'Table' THEN 'Table'
				WHEN substr( object_name, 1, 10	) = 'Projection' THEN 'Projection'
				WHEN object_name = '<Unknown or deleted object>' THEN '<Unknown or deleted object>'
				ELSE object_name
			END = 'Global Catalog'
			AND mode = 'X'
			AND approximate_percentile( duration USING parameters percentile = 0.90	)::int > 1 THEN 'V_ACTION_NICE: We found 10% of transactions are waiting for GCL-X for more than 1 second. Cluster performance may appear slugish.'
		END ACTION 
	FROM
		(
			SELECT
				object_name
				, mode
				, RESULT
				, max(datediff('second', start_time, time)) duration
			FROM
				${VINT}.dc_lock_attempts
			GROUP BY 1, 2, 3, transaction_id
		) foo
	CROSS JOIN (
			SELECT
				datediff( 'hour', min(time)	, max(time)	) History_hours
			FROM
				${VINT}.dc_lock_attempts
		) t
	GROUP BY 1, 2, 3
	ORDER BY 1;
	
	\echo '    Step 15b: Additional Testing Queries - LockHolds Stats (VAdvisor format)'
    \qecho >>> Step 15b: Additional Testing Queries - LockHolds Stats (VAdvisor format)
	
	-- Lock release
	SELECT
		/*+label(lockHoldStatsByLockType)*/
		CASE
			WHEN substr( object_name, 1, 5 ) = 'Table' THEN 'Table'
			WHEN substr( object_name, 1, 10	) = 'Projection' THEN 'Projection'
			WHEN object_name = '<Unknown or deleted object>' THEN '<Unknown or deleted object>'
			ELSE object_name
		END
		, mode
		, count(*)
		, min(duration) min_hold
		, approximate_percentile( duration USING parameters percentile = 0.90 )::int AS percentile_90_hold
		, max(duration) max_hold
		,
		CASE
			WHEN (
				CASE
					WHEN substr( object_name, 1, 5	) = 'Table' THEN 'Table'
					WHEN substr( object_name, 1, 10 ) = 'Projection' THEN 'Projection'
					WHEN object_name = '<Unknown or deleted object>' THEN '<Unknown or deleted object>'
					ELSE object_name
				END = 'Global Catalog'
				AND mode = 'X'
				AND min(t.History_hours) < 24
			) THEN 'dc_lock_releases holding less than ' || min(t.History_hours)+ 1 || ' hour(s) of historical data. An ideal retention history is atleast 24 hours. To increase retention, please run select set_data_collector_policy(''LockReleases'',''500'',''500000'');'
			WHEN
			CASE
				WHEN substr( object_name, 1, 5 ) = 'Table' THEN 'Table'
				WHEN substr( object_name, 1, 10	) = 'Projection' THEN 'Projection'
				WHEN object_name = '<Unknown or deleted object>' THEN '<Unknown or deleted object>'
				ELSE object_name
			END = 'Global Catalog'
			AND mode = 'X'
			AND approximate_percentile( duration USING parameters percentile = 0.90	)::int > 1 THEN 'V_ACTION_NICE: We found 10% of transactions are holding for GCL-X for more than 1 second. Cluster performance may appear slugish.'
		END ACTION
	FROM
		(
			SELECT
				object_name
				, mode
				, max(datediff('second', grant_time, time)) duration
			FROM
				${VINT}.dc_lock_releases
			GROUP BY 1, 2, transaction_id
		) foo
	CROSS JOIN (
			SELECT
				datediff( 'hour', min(time)	, max(time) ) History_hours
			FROM
				${VINT}.dc_lock_releases
		) t
	GROUP BY 1, 2 
	ORDER BY 1;
	
	\echo '    Step 15c: Additional Testing Queries - Transaction with GCLX (VAdvisor format)'
    \qecho >>> Step 15c: Additional Testing Queries - Transaction with GCLX (VAdvisor format)
	-- Transaction with GlobalCatalog Lock
	SELECT
		/*+label(transactionHoldingGCLX)*/
		r.transaction_id
		, max(i.description) request
		, min(grant_time)
		, max(DATEDIFF('second', grant_time, r.time)) hold_in_seconds
		, 'released' status
		, max(hours_of_history) hours_of_history
	FROM
		(
			SELECT
				datediff( 'hour', min(time), max(time) ) AS hours_of_history
			FROM
				${VINT}.dc_lock_releases
		) foo
		, ${VINT}.dc_lock_releases r
	LEFT JOIN ${VINT}.dc_transaction_starts i
			USING(transaction_id)
	WHERE
		object_name ilike '%global%catalog%'
		AND mode = 'X'
	GROUP BY 1
	HAVING max(DATEDIFF('second', grant_time, r.time)) > 1
	UNION
	SELECT
		a.transaction_id
		, max(i.description) request
		, min(a.start_time)
		, max(DATEDIFF('SECOND', a.start_time, max_timestamp)) hold_in_seconds
		, 'held' status
		, max(hours_of_history) hours_of_history
	FROM
		${VINT}.dc_lock_attempts a
	LEFT JOIN ${VINT}.dc_transaction_starts i
			USING(transaction_id)
		, (
			SELECT
				max(login_timestamp) max_timestamp
			FROM
				${VMON}.sessions
		) s
		, ( SELECT datediff( 'hour', min(time), max(time) ) AS hours_of_history
			FROM ${VINT}.dc_lock_releases
		) foo
	WHERE
		object_name = 'Global Catalog'
		AND mode = 'X'
		AND NOT EXISTS (
			SELECT
				'x'
			FROM
				${VINT}.dc_lock_releases r
			WHERE
				a.transaction_id = r.transaction_id
				AND r.object_name = 'Global Catalog'
				AND r.mode = 'X'
		)
		AND start_time >= (
			SELECT
				max(time)
			FROM
				${VINT}.dc_lock_releases r
			WHERE
				r.object_name = 'Global Catalog'
				AND r.mode = 'X'
		)
	GROUP BY 1
	ORDER BY 4 DESC
	LIMIT 10;
	
	\echo '    Step 15d: Additional Testing Queries - Projection with data SKEW (VAdvisor format)'
    \qecho >>> Step 15d: Additional Testing Queries - Projection with data SKEW (VAdvisor format)
	
	SELECT
		/*+label(ProjsWithMT10PercentSkewedData)*/
		schema_name AS schema_name
		, projection_name AS projection_name
		, TO_CHAR( Min(cnt), '999,999,999,999' ) AS min_count
		, TO_CHAR( Avg(cnt):: INT, '999,999,999,999' ) AS avg_count
		, TO_CHAR( Max(cnt), '999,999,999,999' ) AS max_count
		,(( Max(cnt) * 100 / Min(cnt)) - 100) :: INT AS skew_percent
		,
		CASE
			WHEN ((( Max(cnt) * 100 / Min(cnt) ) - 100) :: INT > 50 AND Min(cnt) > 100000000 ) THEN 'V_ACTION_SHOULD: Detected projection with more than 50% data skew, create projection with new segmentation clause to fix data skew, refresh and drop old projection'
		END ACTION
	FROM
		(
			SELECT
				node_name
				, schema_name
				, projection_name
				, Sum(total_row_count - deleted_row_count) AS cnt
			FROM
				${VMON}.storage_containers
			GROUP BY 1, 2, 3
			HAVING Sum(total_row_count - deleted_row_count) > 100000000
		) foo
	GROUP BY 1, 2
	HAVING (( Max(cnt) * 100 / Min(cnt)) - 100 ) :: INT > 15
	ORDER BY 6 DESC
	LIMIT 150;
	
	\echo '    Step 15e: Additional Testing Queries - DisksPercentFull (VAdvisor format)'
    \qecho >>> Step 15e: Additional Testing Queries - DisksPercentFull (VAdvisor format)
    SELECT
        /*+label(DisksPercentFull)*/
        ds.node_name
        , COALESCE (n.subcluster_name, 'Ent.Mode') AS subcluster_name
        , storage_usage , (sum(disk_space_used_mb)* 100 / sum(disk_space_free_mb + disk_space_used_mb))::int storage_usage_percent
        , (sum(disk_space_free_mb + disk_space_used_mb)/1024)::NUMERIC(10,2) capacity_Gb
    FROM
        ${VMON}.disk_storage ds
    JOIN ${VCAT}.nodes n on ds.node_name = n.node_name
    WHERE
        storage_usage <> 'USER'
        AND (
            storage_usage ilike 'DATA%' 
        )
    GROUP BY 1, 2, 3
    HAVING (sum(disk_space_used_mb)* 100 / sum(disk_space_free_mb + disk_space_used_mb))::int > 0 ---- **** change to 50
    ORDER BY 1, 2, 3 ASC, 4 DESC;
	
	\echo '    Step 15f: Additional Testing Queries - TOP 50 Queries using more than 25 percent of memory (VAdvisor format)'
  \qecho >>> Step 15f: Additional Testing Queries - TOP 50 Queries using more than 25 percent of memory (VAdvisor format)
	-- 
  WITH queries25pctmem AS (
	SELECT
		/*+label(Queryusingmorethan50percentofmemory)*/
		ra.transaction_id
		, ra.statement_id
 		, max(memory_kb) memory_consumed_kb
		, ra.succeeded
		, ra.pool_name
		, max(max_memory_kb::float) max_memory_kb
	FROM
		(
			SELECT
				max(total_memory_bytes / 1024) max_memory_kb
			FROM
				${VMON}.host_resources
		) foo
		, ${VINT}.dc_resource_acquisitions ra 
	WHERE
		memory_kb > ( SELECT max((total_memory_bytes / 1024)/ 4)
			FROM ${VMON}.host_resources
		)
	GROUP BY 1, 2, 4, 5
	ORDER BY 3 DESC
	LIMIT 50)
	SELECT  queries25pctmem.transaction_id
	      , queries25pctmem.statement_id
	      , CASE WHEN UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
           ELSE UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) END AS qtype
          , memory_consumed_kb
          , ((memory_consumed_kb // max_memory_kb)*100)::INT  memory_pct
          , succeeded
          , pool_name
          , max_memory_kb
    FROM queries25pctmem
         JOIN ${VMON}.query_requests qr USING (transaction_id,statement_id);
	--
	\echo '    Step 15g: Additional Testing Queries - ROS containers above 256 (VAdvisor format)'
  \qecho >>> Step 15g: Additional Testing Queries - ROS containers above 256 (VAdvisor format)
	WITH storagecnt AS (
		SELECT
			schema_name
			, projection_name
			, projection_id
			, max(storage_cnt1) storage_cnt
		FROM
			( SELECT
					schema_name
					, projection_name
					, projection_id
					, count(DISTINCT storage_oid) storage_cnt1
				FROM
					${VMON}.storage_containers s
				GROUP BY 1, 2, 3, s.node_name
			) foo
		GROUP BY 1, 2, 3 
		HAVING max(storage_cnt1) > 100
		ORDER BY 4 DESC LIMIT 50
	)
	, vspart AS (
		SELECT
			table_schema
			, projection_name
			, projection_id
			, is_null
			, count(DISTINCT partition_key) partkey_cnt
		FROM ${VINT}.vs_partitions
		GROUP BY 1, 2, 3, 4
	)
	, vsstrata AS (
		SELECT
			schema_name
			, projection_name
			, projection_id
			, max(part_keys) active_partitions
			, max(storage_cnt) as active_partition_ros_cnt
		FROM
			( SELECT
					node_name
					, schema_name
					, projection_name
					, projection_id
					, REPLACE(listagg(DISTINCT partition_key USING PARAMETERS max_length = 2048, on_overflow = 'TRUNCATE'), ',', ', ') part_keys
					, count(storage_oid) storage_cnt
				FROM
					vs_strata
				GROUP BY 1, 2, 3, 4
			) foo
		GROUP BY 1, 2, 3)
	SELECT
		/*+label(ROSesPerProjPerNode)*/
		s.schema_name
		, s.projection_name
		, s.storage_cnt
		, p.partkey_cnt
		, st.active_partitions
		, st.active_partition_ros_cnt
		, CASE WHEN substr( nvl( partition_group_expression, ' '), 1, 4 ) = 'CASE'
			THEN 'True'
			ELSE 'False'
		END as is_hierarchical_partition
		,CASE WHEN p.is_null = 't'
				THEN 'V_ACTION_MUST: Found a table with null partition_key, table need to be reorganized'
			WHEN p.is_null = 'f' AND p.partkey_cnt > 256 AND substr( nvl( partition_group_expression, ' ' ), 1 , 4 ) <> 'CASE'
				THEN 'V_ACTION_SHOULD: Found table with more than 256 partitions, create hierarchical partition by adding group by clause to partition expression.'
			WHEN p.is_null = 'f' 
				AND ( ( s.storage_cnt - st.active_partition_ros_cnt) / CASE WHEN p.is_null = 'f' AND nvl( p.partkey_cnt, 1 ) = 0 	THEN 1 	ELSE partkey_cnt END )::INT > 2
				AND s.storage_cnt - st.active_partition_ros_cnt > 256
				THEN 'V_ACTION_SHOULD: Table has high ROS count for inactive partitions. Review partition expression and ETL to make sure you are not loading data in inactive partitions.'
			END AS action_should
		FROM storagecnt s
		LEFT JOIN vspart p ON s.projection_id = p.projection_id
		LEFT JOIN vsstrata st ON s.projection_id = st.projection_id
		INNER JOIN (
			SELECT DISTINCT anchortable
				, oid
			FROM ${VINT}.vs_projections
		) vp ON vp.oid = s.projection_id
		INNER JOIN ${VINT}.vs_tables_view ON anchortable = table_id
		ORDER BY 3 DESC;
			
  
  \echo '    Step 15h: Additional Testing Queries - Connections Initiated(VAdvisor format)'
  \qecho >>> Step 15h: Additional Testing Queries - Connections Initiated(VAdvisor format)
	
        select /*+label(isLoadBalanced)*/ right(a.node_name,3)::int as node_num
         , NVL(subcluster_name, 'Ent.Mode')||CASE WHEN c.is_primary and subcluster_name||'XXX' != 'XXX'
         THEN ' (P)' ELSE '' END  as subcl_name
         , count( case when request_type ='LOAD' then 1 else null end ) as loads_initiated
         , count( case when request_type ='QUERY' then 1 else null end ) as selects_initiated 
        from ${VINT}.dc_requests_issued a 
        join ${VINT}.vs_nodes b on a.node_name = b.name and b.nodetype = 0 
        join ${VCAT}.nodes c on a.node_name = c.node_name
        group by 1,2 order by 2,1;
	
	\echo '    Step 15i: Additional Testing Queries - LOAD_STREAMS'
  \qecho >>> Step 15i: Additional Testing Queries - LOAD_STREAMS

	SELECT
	STREAM_NAME
	, SCHEMA_NAME || '.' || TABLE_NAME AS OBJECT_NAME
	, LOAD_START
	, (	LOAD_DURATION_MS // 1000 ) AS LOAD_DURATION_SEC
	, ACCEPTED_ROW_COUNT
	, REJECTED_ROW_COUNT
	, READ_BYTES
	, UNSORTED_ROW_COUNT
	, SORTED_ROW_COUNT
	FROM
	(SELECT
    table_accepted_row_count.session_id
    , table_accepted_row_count.transaction_id
    , table_accepted_row_count.statement_id
    , stream_name
    , schema_name
    , table_id
    , table_name
    , load_start
    , load_duration_ms
    , is_executing
    , accepted_row_count
    , rejected_row_count
    , CASE
        WHEN total_rows = num_not_nulls
        AND input_size < read_bytes THEN input_size
        ELSE read_bytes
    END AS read_bytes
    , CASE
        WHEN total_rows = num_not_nulls THEN input_size
        ELSE NULL
    END AS input_file_size_bytes
    , CASE
        WHEN total_rows = num_not_nulls
        AND input_size < read_bytes THEN  
               CASE
            WHEN input_size = 0 THEN NULL
            ELSE input_size * 100 // input_size
        END
        WHEN total_rows = num_not_nulls
        AND input_size > 0 THEN read_bytes * 100 // input_size
        ELSE NULL
    END AS parse_complete_percent
    , unsorted_row_count
    , sorted_row_count
    , (CASE
        WHEN unsorted_row_count > 0 THEN sorted_row_count * 100 // unsorted_row_count
        ELSE NULL
    END) AS sort_complete_percent
    FROM 
    (
    SELECT
        transaction_id
        , statement_id
        , session_id
        , identifier AS stream_name
        , query_start AS load_start
        , query_duration_us // 1000 AS load_duration_ms
        , is_executing
    FROM
        ${VMON}.query_profiles AS q
    WHERE
        query_type = 'LOAD'
        AND (error_code = 0
            OR error_code IS NULL
            OR (
            SELECT
                max(error_level)
            FROM
                ${VINT}.dc_errors AS e
            WHERE
                q.session_id = e.session_id
                AND 
                                          q.transaction_id = e.transaction_id
                AND 
                                          q.statement_id = e.statement_id) < 20)) AS query_profiles
NATURAL JOIN 
    (
    SELECT
        transaction_id
        , statement_id
        , session_id
        , sum(CASE WHEN operator_name = 'Load' AND counter_name = 'rows produced' AND counter_tag = '' THEN counter_value ELSE 0 END) AS accepted_row_count
        , sum(CASE WHEN operator_name = 'Load' AND counter_name = 'rows rejected' AND counter_tag = '' THEN counter_value ELSE 0 END) AS rejected_row_count
        , sum(CASE WHEN operator_name = 'Load' AND counter_name = 'read (bytes)' AND (counter_tag = 'worker' OR counter_tag = 'main') THEN counter_value ELSE 0 END) AS read_bytes
        , sum(CASE WHEN counter_tag = 'main' AND counter_name = 'input size (bytes)' AND (operator_name = 'Load' OR operator_name = 'LoadUnion') THEN 1 ELSE 0 END) AS total_rows
        , count(CASE WHEN counter_tag = 'main' AND counter_name = 'input size (bytes)' AND (operator_name = 'Load' OR operator_name = 'LoadUnion') THEN counter_value ELSE NULL END) AS num_not_nulls    -- count(x) ignores null values of x.
        , sum(CASE WHEN counter_tag = 'main' AND counter_name = 'input size (bytes)' AND (operator_name = 'Load' OR operator_name = 'LoadUnion') THEN counter_value ELSE 0 END) AS input_size
        , sum(CASE WHEN counter_name = 'input rows' AND operator_name = 'DataTarget' THEN counter_value ELSE 0 END) AS unsorted_row_count
        , sum(CASE WHEN counter_name = 'written rows' AND operator_name = 'DataTarget' THEN counter_value ELSE 0 END) AS sorted_row_count
    FROM
        ${VMON}.execution_engine_profiles
    GROUP BY transaction_id, statement_id, session_id) AS table_accepted_row_count
    NATURAL JOIN 
    (
    SELECT
        DISTINCT session_id
        , transaction_id
        , statement_id
        , table_schema AS schema_name
        , table_oid AS table_id
        , table_name
    FROM
        ${VINT}.dc_projections_used) AS table_schema_name) load_streams
	WHERE
	NOT IS_EXECUTING;										
 
 
 	\echo '    Step 15j: Additional Testing Queries - Mergeout Operations Ratio'
  \qecho >>> Step 15j: Additional Testing Queries - Mergeout Operations Ratio
 
  SELECT /*+label(PercentageOfNonStrataMergeouts)*/
    containers_merged, count(*) count, count(distinct proj_name) distinct_projections 
  FROM 
    (SELECT schema_name || '.' || projection_name proj_name,
      CASE when s.container_count >= 32 then 'Strata based mergeout' 
      else 'non-strata based mergeout' END containers_merged
      FROM ${VINT}.dc_tuple_mover_events s
      WHERE s.event = 'Complete' and s.plan_type = 'Mergeout' and s.container_count > 1) foo 
      GROUP BY 1 ORDER BY 1 desc;
 
 
  
  \echo '    Step 16a: EON Specific - Subclusters Info'
  \qecho >>> Step 16a: EON Specific - Subclusters Info

  SELECT
    /*+label(subcluster)*/
      sb.subclustername
    , sb.name
    , sb.isprimary primary_node
    , sb.max_size_in_gb
    , sb.current_size_in_gb
    , sb.pct_used
    , SUM(CASE WHEN ns.state = 'ACTIVE' THEN 1 ELSE 0 END) active_subscriptions
    , COUNT(*) total_subscriptions
    , CASE
        WHEN (sb.max_size_in_gb * 100) // (disk_space_total_mb // 1024) > 80 THEN 'V_ACTION_SHOULD: Current maximum depot size (' || sb.max_size_in_gb || ' GB) is greater than 80% of local disk (' || disk_space_total_mb // 1024 || ' GB). Please set max depot size to 80% by using ALTER_LOCATION_SIZE function.'
        WHEN (ns.state <> 'ACTIVE') THEN 'V_ACTION_MUST: Found one or more subscription in not ACTIVE state at the time when scrutinize was collected'
        WHEN (h.disk_space_total_mb // 1024 - sb.max_size_in_gb) < 256 THEN 'V_ACTION_SHOULD: Set size depot location to leave minimum 256GB of space of non depot locations like CATALOG and TEMP. TEMP location is used by temporary tables and sort operations that spill to disk.'
        WHEN h.disk_space_total_mb // 1024 < 1024 THEN 'V_ACTION_NICE: Vertica recommends local disk of at least 1TB per node out of which 80% can be reserved for Depot.'
        WHEN sb.alert_depot_size = TRUE THEN 'V_ACTION_SHOULD: Depot size should be the same across the nodes in one subcluster.'
    END ACTION
  FROM
    (
    SELECT
        oid
        , name
        , address
        , subclustername
        , isprimary
        , max_size_in_gb
        , current_size_in_gb
        , pct_used
        , MAX(alert_depot_size) alert_depot_size
    FROM
        (
        SELECT
            n.oid
            , n.name
            , n.address
            , s.subclustername
            , s.isprimary
            , (vs.max_size_in_bytes / 1024 ^ 3)::NUMERIC(20,2) max_size_in_gb
            , (vs.current_size_in_bytes / 1024 ^ 3)::NUMERIC(20,2) current_size_in_gb
            , (vs.current_size_in_bytes/GREATEST (vs.max_size_in_bytes,1)*100)::NUMERIC(5,2) AS pct_used
            , CASE
                WHEN (ABS(vs.max_size_in_bytes - MEDIAN(vs.max_size_in_bytes) OVER(PARTITION BY s.subclustername)) / MEDIAN(vs.max_size_in_bytes) OVER(PARTITION BY s.subclustername) * 100 > 10) THEN TRUE
                ELSE FALSE
            END alert_depot_size
        FROM
            ${VINT}.vs_nodes n
        JOIN ${VINT}.vs_node_states s1 ON
            s1.node_oid = n.oid
        JOIN ${VINT}.vs_subclusters s ON
            s.subclusteroid = s1.subcluster_oid
        JOIN ${VINT}.vs_depot_size vs ON
            vs.node_name = n.name) sb1
    GROUP BY 1,2,3,4,5,6,7,8) sb
    JOIN vs_node_subscriptions ns ON ns.nodeoid = sb.oid
    LEFT JOIN host_resources h ON h.host_name = sb.address
    GROUP BY 1,2,3,4,5,6,9
    ORDER BY 1,2;
      
      
   \echo '    Step 16b: EON Specific - Shards Subscription Info'
   \qecho >>> Step 16b: EON Specific - Shards Subscription Info

    SELECT
    /*+label(shards)*/
    parent_name AS subcluster
    , max(s.shardcnt) shards
    , sum(node_count) number_of_nodes
    , sum(subscription) AS subscriptions
    , sum(active_subscriptions) active_subscriptions
    , CASE
        WHEN count(DISTINCT shards) = 1 THEN TRUE
        ELSE FALSE
    END are_subscription_balanced
    , sum(primary_subscriptions) primary_subscriptions
    , count(DISTINCT control_nodes) control_nodes
    , CASE
        WHEN max(shards) <> min(shards) THEN 'V_ACTION_MUST: Subscriptions are not balanced across all nodes in the subcluster. Please run rebalance_shards and pass subcluster name as argument'
        WHEN sum(subscription)// sum(node_count) <> sum(active_subscriptions)// sum(node_count) THEN 'V_ACTION_MUST: Some subscriptions found in no ACTIVE state'
        WHEN max(s.shardcnt) < sum(node_count) THEN 'V_ACTION_SHOULD: Subcluster has more nodes than shard count. Vertica recommends not creating a subcluster with node count greater than shard count. Please create new subcluster with additional nodes to get elastic throughput scaling(ETS).'
        WHEN max(s.shardcnt)%count(DISTINCT control_nodes) <> 0 THEN 'V_ACTION_MUST: Number of control nodes is not factor of nodes in the cluster, please change number of control nodes for this subcluster'
    END ACTION
    FROM
    (
      SELECT
        s.subclustername parent_name
        , node_name
        , count(DISTINCT vs_shards.shardname) shards
        , count(DISTINCT node_name) node_count
        , count(*) subscription
        , count( CASE WHEN ns.type = 'PRIMARY' THEN 1 ELSE NULL END) primary_subscriptions
        , count( CASE WHEN state = 'ACTIVE' THEN 1 ELSE NULL END) active_subscriptions
        , parentfaultgroupid control_nodes
      FROM
        ${VINT}.vs_subclusters s
      JOIN ${VINT}.vs_node_states n ON
        s.subclusteroid = n. subcluster_oid
      JOIN ${VCAT}.vs_node_subscriptions ns ON
        ns.nodeoid = n.node_oid
      JOIN ${VINT}.vs_shards ON
        vs_shards.oid = ns.shardoid
      JOIN ${VINT}.vs_nodes ON
        name = n.node_name
      GROUP BY 1, 2, 8
    ORDER BY 1, 2 ) foo
    , (SELECT count(*) AS shardcnt FROM ${VINT}.vs_shards WHERE shardname <> 'replica' ) s
    GROUP BY 1
    ORDER BY 1;
  
  
   \echo '    Step 16c: EON Specific - Repetition of file(s) refetches'
   \qecho >>> Step 16c: EON Specific - Repetition of file(s) refetches  
    
    SELECT
    /*+label(refetches)*/
    node_name
    , storageid
    , history_hours
    , count(*) cnt_of_refetches
    , CASE
        WHEN count(*) > 2 THEN 'V_ACTION_SHOULD: File is refetched into depot more than once in 7 days indicate depot size is not sufficient or depot thrashing by queries. You may also consider depot pinning feature to pin table or partition to depot.'
    END
    FROM ${VINT}.dc_depot_fetches
      , (
      SELECT
        datediff('hour', min(time), max(time)) history_hours
      FROM
        ${VINT}.dc_depot_fetches) foo
      WHERE time > (
        SELECT
            max(time) - INTERVAL '7 days'
        FROM ${VINT}.dc_depot_fetches )
      GROUP BY 1, 2, 3
      HAVING count(*) > 1 
      ORDER BY 4 DESC, 1
    LIMIT 50;
  

   \echo '    Step 16d: EON Specific - Depot pining structure'
   \qecho >>> Step 16d: EON Specific - Depot pining structure  

   WITH depot_content AS (
    SELECT df.node_name
        , schema_name AS table_schema
        , sc.projection_name
        , anchor_table_name AS table_name
        , sum(file_size_bytes) AS total_filesize_in_depot
        , sum(used_bytes)   AS total_used_by_projection
        , sum(total_row_count) AS total_row_count
        , is_pinned
         FROM 
        ( SELECT 
            node_name, 
            sal_storage_id, 
            storageContainerOid as storage_oid, 
            communal_file_path, 
            depot_file_path, 
            shard_name, 
            storage_type, 
            num_accesses as number_of_accesses, 
            size as file_size_bytes, 
            last_access_time, 
            arrival_time, 
            source, 
            is_pinned 
          FROM ${VINT}.vs_depot_lru, ${VCAT}.shards where vs_depot_lru.shard_oid=shards.shard_oid
        ) df
        LEFT JOIN ${VMON}.storage_containers sc USING (SAL_STORAGE_ID)
        JOIN ${VCAT}.projections p USING (projection_id)
        GROUP BY 1,2,3,4, is_pinned
    )  
    SELECT
      subcluster_name
    , category
    , is_pinned
    , count(*) count_of_tables
    FROM
      (
      SELECT
        CASE
            WHEN sum_depot_bytes <= 1024^3 THEN 'lt1GB'
            WHEN sum_depot_bytes <= 500*1024^3  THEN 'lt500GB'
            WHEN sum_depot_bytes <= 1024*1024^3 THEN 'lt1TB'
            ELSE 'gt1TB'
        END AS category
        , table_schema
        , table_name
        , subcluster_name
        , is_pinned
      FROM
        (
          SELECT
             table_schema
            , table_name
            , subcluster_name
            , is_pinned
            , sum(total_filesize_in_depot) AS sum_depot_bytes
        FROM
            depot_content content
        INNER JOIN ${VCAT}.subclusters sub ON
            sub.node_name = content.node_name
        WHERE
            total_filesize_in_depot > 0
        GROUP BY
            1, 2, 3, 4) TE 
        ) a
    GROUP BY 1,2,3;
    
    
  -- ------------------------------------------------------------------------
  -- Script End Time
  -- ------------------------------------------------------------------------
    \echo '    Step 17: Script End Timestamp'
    \qecho >>> Step 17: Script End Timestamp
    SELECT
        SYSDATE() AS 'End Timestamp'
    ;
EOF

#---------------------------------------------------------------------------
# GZIP output file
#---------------------------------------------------------------------------
test ${GZP} -eq 1 && { echo "Gzipping ${OUT}" ; gzip -f ${OUT} ;}

#------------------------------------------------------------------------
# End of sprof
#------------------------------------------------------------------------
secs=$(( `date +%s` - secs ))
hh=$(( secs / 3600 ))
mm=$(( ( secs / 60 ) % 60 ))
ss=$(( secs % 60 ))
printf "[%s] ${SPV} completed in %d sec (%02d:%02d:%02d)\n" "`date +'%Y-%m-%d %H:%M:%S'`" ${secs} $hh $mm $ss

exit 0
