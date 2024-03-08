#!/bin/bash

#--------------------------------------------------------------------------
# sprofpp version 0.5a
# post processor for sprof-0.5* - Jun 2022
# vim: et:ts=4:sw=4:sm:ai:syn=sh
#--------------------------------------------------------------------------

#---------------------------------------------------------------------------
# Step 0: Initialization
#---------------------------------------------------------------------------

# Colors (using hex values for escape because Mac bash does not support \e):
DONE="\x1B[0;32mdone\x1B[00m"
FAIL="\x1B[0;31mfailed\x1B[00m"
WARN="\x1B[0;33mCompleted with Warnings\x1B[00m"

# Default values:
SPPV="sprofpp version 0.5a - Jun 2022"
PNAME="project"
GZP=0
CLEAN=false
PNGWIDTH=12.12	# PNG Graph Width (inch)
PNGHEIGHT=6.06	# PNG Graph Height (inch)
PNGRES=96 	 	# PNG Graph Resolution
CAGGR=hours     # Default Concurrency plot aggregation level
PPTEMPL="$(dirname $0)/templates/master.pptx"

#Max rows in table per slide 
MAXROWS=20

usage="Usage: sprofpp [-p project_name] [-W width] [-H height] [-R res] [-h] [-c] [-t template PPTX file] [--help] -f sprof_output_file\n"
usage+="    -p project_name to set the project name (default \'project\')\n"
usage+="    -W width PNG Graphs Width (default 12.12)\n"
usage+="    -H height PNG Graphs Height (default 6.06)\n"
usage+="    -R resolution PNG Graphs Resolution (default 96)\n"
usage+="    -a {none|secs|mins|hours} concurrency aggregation level (default hours)\n"
usage+="    -c clean temporary files\n"
usage+="    -t location to a PPTX template file based on which PPTX is generated. Default location: '$(dirname $0)/templates/master.pptx' \n"
usage+="    -h | --help print this message"


test $# -eq 0 && { echo -e $usage ; exit ; }
while [ $# -gt 0 ]; do
    case "$1" in
        "-p")
            PNAME="$2"
            shift 2
            ;;
        "-f")
            SPOUT=$2
			test -f ${SPOUT} || { echo "Cannot read ${SPOUT}" ; exit 1 ; }
            shift 2
            ;;
        "-t")
            PPTEMPL=$2
			test -f ${PPTEMPL} || { echo "Cannot read ${PPTEMPL}" ; exit 1 ; }
            shift 2
            ;;         
        "-W")
            PNGWIDTH=$2
            shift 2
            ;;
        "-H")
            PNGHEIGHT=$2
            shift 2
            ;;
        "-R")
            PNGRES=$2
            shift 2
            ;;
        "-a")
            CAGGR=$2
            shift 2
            echo "none secs mins hours" | grep -qw ${CAGGR} || { echo invalid aggregation level ; exit ; }
            ;;
        "-c")
            CLEAN=true
			test -f ${SPOUT} || { echo "Cannot read ${SPOUT}" ; exit 1 ; }
            shift
            ;;
        "--help" | "-h")
            echo -e $usage
            exit 0
            ;;
        *)
            echo "[sprofpp] invalid option '$1'"
            echo -e $usage
            exit 1
            ;;
    esac
done

sptype=$(file --mime-type ${SPOUT} | cut -f2 -d:)
case ${sptype} in
	" application/x-gzip" | " application/gzip" )
		GZP=1
		GREP=$(which zgrep)
		test $(uname -s) = "Linux" && CAT=$(which zcat)
		test $(uname -s) = "Darwin" && CAT=$(which gzcat)
		;;
	" text/plain" )
		GZP=0
		GREP=$(which grep)
		CAT=$(which cat)
		;;
	* )
		echo "Unknown format '${sptype}' for ${SPOUT}. Only plain text and gzip are supported"
		exit 1
		;;
esac

RSCRIPT=$(which Rscript)
test -z ${RSCRIPT} && { echo "Rscript not found" ; exit 1 ; }

#---------------------------------------------------------------------------
# Step 1: General Information:
#---------------------------------------------------------------------------
OUT=${PNAME}'_Info.txt'
echo -n "01 - Generic Information... "

${GREP} -m 1 "Vertica Analytic Database" ${SPOUT} > ${OUT}

echo "Information|Value" > ${PNAME}_System.txt
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 1b:/,/^>>> Step 1c:|^>>> Step 2:/{ if(NF==2) print }' >> ${PNAME}'_System.txt' 
    
 

${GREP} -m 1 "Raw Data Size" ${SPOUT} >> ${OUT}



#MP: clearing file  ${PNAME}'_Host.txt' before starting 
if test -f ${PNAME}'_Host.txt'; then
    > ${PNAME}'_Host.txt'
fi
${CAT} ${SPOUT} | awk -F\| '
    BEGIN{row="host|cores|CPU|memory(MB)|Disk Space(GB)"} 
    /^>>> Step 5a:/,/^>>> Step 5b:/{
        if(NF==2){if($1~/host_name/){print row;row=$2}; 
                  if($1~/processor_core_count/){row=row "|" $2 }; 
                  if($1~/processor_description/){row=row "|" $2 }; 
                  if($1~/total_memory_bytes/){row=row "|" ($2/1024/1024/1024)}; 
                  if($1~/disk_space_total_mb/){row=row "|" ($2/1024)} 
        } 
        if($1~/Step 5b:/){print row;exit} 
    }' >> ${PNAME}'_Host.txt' 



#MP: clearing file  ${PNAME}'_ConfParam.txt' before starting 
if test -f ${PNAME}'_ConfParam.txt'; then
    > ${PNAME}'_ConfParam.txt'
fi	
${CAT} ${SPOUT} | awk -F\| '
    BEGIN{row="Parameter|Current Value|Default Value|Description"}
    /^>>> Step 3:/,/^>>> Step 4:/{
        if(NF==2){
            if($1~/parameter_name/){print row;row=$2} 
            else row=row "|" $2
        };
        if($1~/Step 4:/){print row;exit}
    }' >> ${PNAME}'_ConfParam.txt' 

echo -e
echo -e "		Information"
echo "Option|Value" > ${PNAME}_sprof.txt
${CAT} ${SPOUT} | awk -F\: '{if(NR>9) exit; sub(/^### /,""); if(NF==2) print $1"|"$2;}' >> ${PNAME}_sprof.txt
${GREP} -A2 "Start Timestamp" ${SPOUT} | tail -n1 | sed 's/^/Start Timestamp|/' >> ${PNAME}_sprof.txt
${GREP} -A2 "End Timestamp" ${SPOUT} | tail -n1 | sed 's/^/End Timestamp|/' >> ${PNAME}_sprof.txt
echo "Information:Value" > ${OUT}
${GREP} -A2 "VERSION" ${SPOUT} | tail -n1 | sed 's/^/Version: /' >> ${OUT}
${GREP} -A2 "GET_DATA_COLLECTOR_POLICY" ${SPOUT} | tail -n1 | sed 's/^/DC Policy: /' >> ${OUT}
${GREP} -A2 "Raw Data Size:" ${SPOUT} | sed 's/^ *//' >> ${OUT}
${GREP} -A2 "Database mode" ${SPOUT} |  tail -n1 | sed 's/^/Database mode: /' >> ${OUT}




echo -e "		Detailed Collector configuration (for all components queried by sproff)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 2b:/,/^>>> Step 3:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_DC.txt

echo -e "		Resource Pools configuration"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 4:/,/^>>> Step 5a:/{if(NF>1)print;}' > ${PNAME}_RP.txt

echo -e "		Slow Events"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 5d:/,/^>>> Step 6a:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_SlowEvent.txt

echo -e "		Database Size (compressed by schema)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 6b:/,/^>>> Step 6c:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_SizeSchema.txt

echo -e "		Database Size (size distribution between table types)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 6c:/,/^>>> Step 6d:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_SizeTypes.txt 

echo -e "		Database Size (size distribution between table types)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 6d:/,/^>>> Step 7a:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_Compression.txt  

echo -e "		Catalog Analysis (Column types)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7a:/,/^>>> Step 7b:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_ColumnType.txt 


echo -e "		Catalog Analysis (Top 10 Largest Schemas)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7b:/,/^>>> Step 7c:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' | head -11 > ${PNAME}_LargeSchema.txt 

echo -e "		Catalog Analysis (Top 10 Tables with more columns)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7c:/,/^>>> Step 7d:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' | head -11 > ${PNAME}_TableColumns.txt

echo -e "		Catalog Analysis (Top 10 Tables with largest rows)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7d:/,/^>>> Step 7e:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' | head -11 > ${PNAME}_TableRows.txt

echo -e "		Catalog Analysis (Top 10 largest segmented projections)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7e:/,/^>>> Step 7f:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' | head -11 > ${PNAME}_SegProj.txt

echo -e "		Catalog Analysis (Top 10 largest unsegmented projections)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7f:/,/^>>> Step 7g:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' | head -11 > ${PNAME}_RepProj.txt

echo -e "		Catalog Analysis (Top 10 less used projections)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7h:/,/^>>> Step 7i:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' | head -11 > ${PNAME}_UsedProj.txt

echo -e "		Catalog Analysis (Tables per schema)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7i:/,/^>>> Step 7j:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_TotalSchema.txt

echo -e "		Catalog Analysis (Catalog Size)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7j:/,/^>>> Step 7k:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_CatalogSize.txt

echo -e "		Catalog Analysis (Number of tables)"
echo "Object Type: Total Number" >  ${PNAME}_Objects.txt
${GREP} -A2 "num_tables" ${SPOUT} | tail -n1 | sed 's/^/Tables: /' >> ${PNAME}_Objects.txt
${GREP} -A2 "num_projections" ${SPOUT} | tail -n1 | sed 's/^/Projections: /' >> ${PNAME}_Objects.txt
${GREP} -A2 "num_columns" ${SPOUT} | tail -n1 | sed 's/^/Columns: /' >> ${PNAME}_Objects.txt

echo -e "		Catalog Analysis (Number of projection basenames by table)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7m:/,/^>>> Step 7n:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_BaseName.txt

echo -e "		Catalog Analysis (Number of delete vectors)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7o:/,/^>>> Step 7p:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_NumDV.txt

echo -e "		Catalog Analysis (Columns by statistics)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7p:/,/^>>> Step 7q:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_ColumnStats.txt

echo -e "		Catalog Analysis (Data types by encoding)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7q:/,/^>>> Step 7r:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_ColumnEnc.txt
	
echo -e "		Catalog Analysis (Number of storage containers)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7r:/,/^>>> Step 7s:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_SCont.txt

echo -e "		Catalog Analysis (Projection basenames by Creation type)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7s:/,/^>>> Step 8a:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_ProjCreationType.txt

echo -e "		Query Events Analysis (Events by Request type)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 8d:/,/^>>> Step 8e:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_QERequest.txt

echo -e "		Query Events Analysis (Events by Statement type)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 8e:/,/^>>> Step 9a:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_QEStatement.txt

echo -e "		Query Elapsed distribution overview"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 9a:/,/^>>> Step 9b:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_QueryET.txt

echo -e "		Statements Execution percentile"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 9c:/,/^>>> Step 9d:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_QueryEP.txt

echo -e "		Lock Attempts Overview"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 12a:/,/^>>> Step 12b:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_LockAttemp.txt

echo -e "		TM durations"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 14b:/,/^>>> Step 14c:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_TMDurations.txt

echo -e "		Long Mergeouts"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 14c:/,/^>>> Step 14d:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_TMLongM.txt

echo -e "		Long Replay Delete"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 14d:/,/^>>> Step 15a:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_TMLongRD.txt

## -- added by MP (Resource Queue Waiting)

echo -e "		Queue wait time overview"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 10b:/,/^>>> Step 11a:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_QueueWait.txt

echo -e "		LockAttempts (VAdvisor format)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15a:/,/^>>> Step 15b:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_LockAttemptsVA.txt

echo -e "		LockHolds Stats (VAdvisor format)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15b:/,/^>>> Step 15c:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_LockHoldsVA.txt

echo -e "		Transaction with GCLX"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15c:/,/^>>> Step 15d:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_TXGCLX.txt

echo -e "		Projection with data SKEW"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15d:/,/^>>> Step 15e:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_PrjSkewness.txt

echo -e "		Disks Percent Full"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15e:/,/^>>> Step 15f:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_DskPctFull.txt

echo -e "		Queries using more than 25 percent of memory"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15f:/,/^>>> Step 15g:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_Over25pctMem.txt

echo -e "		ROS containers above 256 (VAdvisor format)"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15g:/,/^>>> Step 15h:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_ROS256.txt

echo -e "		Connections Initiated per node"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15h:/,/^>>> Step 15i:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_connectionbalancing.txt

echo -e "		Load Streams"
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 15i:/,/^>>> Step 16:/{ sub(/^ */,""); gsub(/ *\| */,"|",$0); if(NF>1) print}' > ${PNAME}_loadstreams.txt


echo -e "${DONE}"

#---------------------------------------------------------------------------
# Step 2: Workload Graph:
#---------------------------------------------------------------------------
echo -n "02 - Extracting workload data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 8b:/,/^>>> Step 8c:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==10) print
    }' > ${PNAME}_wload.txt
echo -e "${DONE}"
echo -n "02 - Generating Workload Analysis R Script... "
cat > wload.R <<-EOF
	#!${RSCRIPT}

	# Load libraries
	library(ggplot2)
	library(scales)
	library(dplyr)
#	library(grid)
  library(gridExtra)


	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_wload.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Transform time_slice to Posix timestamp:
	df[,1] <- as.POSIXct(df[,1], tz="UTC")

	# Produce a new Data Frame Grouping count and avg_ms on time_slice and
	# request_type:
	df2 <- df %>%
	    select(time_slice, request_type, count, avg_ms) %>%
	    group_by(time_slice, request_type) %>%
	    summarise(count=sum(count), avg_s=sum(avg_ms/1000*count)/sum(count))

	# Create two plots for count and avg_s grouped by request_type:
	p1 <- ggplot(data=df2, aes(x=time_slice, y=count/1000, group=request_type, colour=request_type)) +
		  geom_step() +
		  ylab("Request Count (x1000)") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="none")
	p2 <- ggplot(data=df2, aes(x=time_slice, y=avg_s, group=request_type, colour=request_type)) +
		  geom_step() +
		  ylab("Average Elapsed (s)") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="bottom",
				legend.direction="horizontal", legend.title=element_blank()) +
		  guides(colour=guide_legend(nrow=1))

	# Stack the two plots into a 16:9 PNG file
	# MP: grid.newpage()- not needed with gridExtra, 
 
	ggsave("${PNAME}_wload.png", path=getwd(),  plot=grid.arrange(p1, p2) , dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

	# Another Data Frame Grouping count and avg_ms on time_slice and
	# qtype (first word of the query) when request_type = 'QUERY' 
	df3 <- df %>%
	select(time_slice, request_type, qtype, count, avg_ms) %>%
	filter(request_type == "QUERY")%>%
	group_by(time_slice, qtype) %>%
	summarise(count=sum(count), avg_s=sum(avg_ms/1000*count)/sum(count))

	# Create two plots for count and avg_s grouped by request_type:
	p3 <- ggplot(data=df3, aes(x=time_slice, y=count/1000, group=qtype, colour=qtype)) +
		  geom_step() +
		  ylab("Request Count (x1000)") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="none")
	p4 <- ggplot(data=df3, aes(x=time_slice, y=avg_s, group=qtype, colour=qtype)) +
		  geom_step() +
		  ylab("Average Elapsed (s)") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="bottom",
				legend.direction="horizontal", legend.title=element_blank()) +
		  guides(colour=guide_legend(nrow=1))

	# Stack the two plots into a 16:9 PNG file
	# MP: grid.newpage()- not needed with gridExtra, 
	ggsave("${PNAME}_wload2.png", path=getwd(), plot=grid.arrange(p3, p4) , dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "02 - Producing workload graphs... "
${RSCRIPT} ./wload.R >./wload.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./wload.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_wload.png, ${PNAME}_wload2.png]" ; }
test ${CLEAN} = true && rm -f ./wload.R ./wload.R.out ./${PNAME}_wload.txt

#---------------------------------------------------------------------------
# Step 3: Concurrency Graph:
#---------------------------------------------------------------------------
echo -n "03 - Extracting concurrency data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 10:/,/^>>> Step 10b:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==3 && $1!="NULL") print
    }' > ${PNAME}_concurrency.txt
echo -e "${DONE}"
echo -n "03 - Generating Concurrency Analysis R Script... "
if [ ${CAGGR} == "none" ] ; then
	cat > concurrency.R <<-EOF
		#!${RSCRIPT}
		library(ggplot2)
		library(scales)
		df <- read.table("./${PNAME}_concurrency.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")
		options(digits.secs=6)
		df[,1] <- as.POSIXct(df[,1], tz="UTC")
		tval <- 10+round(max(df[,3], na.rm = TRUE))

		# Produce 16:9 PNG file in output
		p <- ggplot(data=df, aes(x=timestamp, y=concurrency, group=request_type, colour=request_type)) +
			geom_step() +
			ggtitle("${PNAME} Concurrency") +
			xlab("") + ylab("") + ylim(0, tval) +
			theme(panel.background=element_blank(),
				  panel.grid.major=element_line(colour="dimgray"),
				  panel.grid.minor=element_line(colour="lightgray"),
				  legend.position="bottom", legend.direction="horizontal",
				  legend.background=element_blank(),
				  legend.title=element_blank(),
				  axis.text.x = element_text(angle=90,hjust=1)) +
			guides(colour=guide_legend(nrow=1)) +
			# scale_x_datetime(breaks=date_breaks("1 day"), labels=date_format("%d-%m", tz="UTC")) +
			scale_y_continuous(breaks = seq(0, tval, by = c(ifelse(tval == 0, 1, ceiling(tval/40))*10 ), minor_breaks = 1:tval))
       ggsave("${PNAME}_concurrency.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
	EOF
else
	cat > concurrency.R <<-EOF
		#!${RSCRIPT}
		library(ggplot2)
		library(scales)
		library(dplyr)
		df <- read.table("./${PNAME}_concurrency.txt", sep="|", header=TRUE, na.strings="NULL",
                        colClasses=c("POSIXct","character","numeric"))
	EOF
    cat >> concurrency.R <<-'EOF'
	    df$timestamp <- as.POSIXct(round(df$timestamp, units="hours"), tz="UTC")
	EOF
	cat >> concurrency.R <<-EOF
		tval <- 10+round(max(df[,3], na.rm = TRUE))
        df2 <- df %>% 
             select(timestamp, request_type, concurrency) %>% 
             group_by(timestamp, request_type) %>% 
             summarise(concurrency=max(as.numeric(concurrency), na.rm = TRUE))
        rm(df)
		p <- ggplot(data=df2, aes(x=timestamp, y=concurrency, group=request_type, colour=request_type)) +
			geom_line() +
			ggtitle("${PNAME} Concurrency") +
			xlab("") + ylab("") + ylim(0, tval) +
			theme(panel.background=element_blank(),
				  panel.grid.major=element_line(colour="dimgray"),
				  panel.grid.minor=element_line(colour="lightgray"),
				  legend.position="bottom", legend.direction="horizontal",
				  legend.background=element_blank(),
				  legend.title=element_blank(),
				  axis.text.x = element_text(angle=90,hjust=1)) +
			guides(colour=guide_legend(nrow=1)) +
			scale_x_datetime(breaks=date_breaks("1 day"), labels=date_format("%d-%m", tz="UTC")) +
			#scale_y_continuous(breaks=seq(0, tval, by=5), minor_breaks=1:tval)
      scale_y_continuous(breaks = seq(0, tval, by = c(ifelse(tval == 0, 1, ceiling(tval/40))*10 ), minor_breaks = 1:tval))

        rm(df2)
	    ggsave("${PNAME}_concurrency.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
	EOF
fi
echo -e "${DONE}"
echo -n "03 - Producing concurrency graph... "
${RSCRIPT} ./concurrency.R >./concurrency.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./concurrency.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_concurrency.png]" ; }
test ${CLEAN} = true && rm -f ./concurrency.R ./concurrency.R.out ./${PNAME}_concurrency.txt

#---------------------------------------------------------------------------
# Step 4: CPU Idle Graph:
#---------------------------------------------------------------------------
echo -n "04 - Extracting CPU utilization data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 13a:/,/^>>> Step 13b:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==63) print
    }' > ${PNAME}_cpu.txt
echo -e "${DONE}"
echo -n "04 - Generating CPU utilization Analysis R Script... "
cat > cpu_idle.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)
	library(scales)
	df <- read.table("./${PNAME}_cpu.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")
EOF
cat >> cpu_idle.R <<-'EOF'
	idle <- df$idle_microseconds_end_value - df$idle_microseconds_start_value
	df$idle_perc <- 100 *
		( df$idle_microseconds_end_value - df$idle_microseconds_start_value ) /
		( df$idle_microseconds_end_value - df$idle_microseconds_start_value +
		  df$user_microseconds_end_value - df$user_microseconds_start_value +
		  df$nice_microseconds_end_value - df$nice_microseconds_start_value +
		  df$system_microseconds_end_value - df$system_microseconds_start_value +
		  df$io_wait_microseconds_end_value - df$io_wait_microseconds_start_value +
		  df$irq_microseconds_end_value - df$irq_microseconds_start_value +
		  df$soft_irq_microseconds_end_value - df$soft_irq_microseconds_start_value +
		  df$steal_microseconds_end_value - df$steal_microseconds_start_value +
		  df$guest_microseconds_end_value - df$guest_microseconds_start_value )
	df[,1] <- as.POSIXct(df[,1], tz="UTC")
	df[,2] <- substring(df[,2],nchar(df[1,2])-3,nchar(df[1,2]))
EOF
cat >> cpu_idle.R <<-EOF
	p <- ggplot(data=df, aes(x=time, y=idle_perc, group=node_name, colour=node_name)) +
		geom_step() +
		ggtitle("${PNAME} CPU idle percent") +
		xlab("") + ylab("") +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC"))
    ggsave("${PNAME}_cpu_idle.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "04 - Producing CPU utilization graph... "
${RSCRIPT} ./cpu_idle.R >./cpu_idle.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./cpu_idle.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_cpu_idle.png]" ; }
test ${CLEAN} = true && rm -f ./cpu_idle.R ./cpu_idle.R.out ./${PNAME}_cpu.txt

#---------------------------------------------------------------------------
# Step 5: Memory Free Graph:
#---------------------------------------------------------------------------
echo -n "05 - Extracting Memory utilization data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 13b:/,/^>>> Step 13c:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==6) print
    }' > ${PNAME}_mem.txt
echo -e "${DONE}"
echo -n "05 - Generating Memory utilization Analysis R Script... "
cat > free_mem.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)
	library(scales)
	df <- read.table("./${PNAME}_mem.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")
	df[,1] <- as.POSIXct(df[,1], tz="UTC")
	# Node names limited to the last four characters
	df[,2] <- substring(df[,2],nchar(df[1,2])-3,nchar(df[1,2]))
	p <- ggplot(data=df, aes(x=time,
	    y=100*(free_memory_start_value+buffer_memory_start_value+file_cache_memory_start_value)/total_memory_start_value, group=node_name, colour=node_name)) +
		geom_step() +
		ggtitle("${PNAME} memory free+cache+buffers percent") +
		xlab("") + ylab("") +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) 
    ggsave("${PNAME}_free_mem.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "05 - Producing Memory utilization graph... "
${RSCRIPT} ./free_mem.R >./free_mem.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./free_mem.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_free_mem.png]" ; }
test ${CLEAN} = true && rm -f ./free_mem.R ./free_mem.R.out ./${PNAME}_mem.txt

#---------------------------------------------------------------------------
# Step 6: TM Events Graph:
#---------------------------------------------------------------------------
echo -n "06 - Extracting TM events data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 14a:/,/^>>> Step 14b:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==6) print
    }' > ${PNAME}_tm.txt
echo -e "${DONE}"
echo -n "06 - Generating TM events Analysis R Script... "
cat > tm.R <<-EOF
	#!${RSCRIPT}

	# Load libraries
	library(ggplot2)
	library(dplyr)
	library(scales)
	library(grid)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_tm.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Transform time_slice to Posix timestamp:
	df[,1] <- as.POSIXct(df[,1], tz="UTC")

	# Produce a new Data Frame Grouping count and size_mb by operation
	# Basically it sums over nodes. Yes... I could have done it in SQL
	# but I like to have per-node details in the text file...
	df2 <- df %>%
	    select(time_slice, operation, count, size_mb) %>%
	    group_by(time_slice, operation) %>%
	    summarise(count=sum(count), size_mb=sum(size_mb))

	# Create two plots for count and size_mb grouped by request_type:
	p1 <- ggplot(data=df2, aes(x=time_slice, y=count, group=operation, colour=operation)) +
		  geom_step() +
		  ylab("TM Events Count - All Nodes") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="none")
	p2 <- ggplot(data=df, aes(x=time_slice, y=size_mb, group=operation, colour=operation)) +
		  geom_step() +
		  ylab("TM Events size_MB - All Nodes") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="bottom",
				legend.direction="horizontal", legend.title=element_blank()) +
		  guides(colour=guide_legend(nrow=1))

	# Stack the two plots into a 16:9 PNG file
	grid.newpage()
	ggsave("${PNAME}_tm.png", plot=grid.draw(rbind(ggplotGrob(p1), ggplotGrob(p2), size = "last")), dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "06 - Producing TM Events graphs... "
${RSCRIPT} ./tm.R >./tm.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./tm.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_tm.png]" ; }
test ${CLEAN} = true && rm -f ./tm.R ./tm.R.out ./${PNAME}_tm.txt

#---------------------------------------------------------------------------
# Step 7: Lock Attempts Graph:
#---------------------------------------------------------------------------
echo -n "07 - Extracting Lock Attempts data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 12b:/,/^>>> Step 13a:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==3) print
    }' > ${PNAME}_locks.txt
echo -e "${DONE}"
echo -n "07 - Generating Lock Attempts Analysis R Script... "
cat > locks.R <<-EOF
	#!${RSCRIPT}

	# Load libraries
	library(ggplot2)
	library(scales)
	library(grid)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_locks.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Transform time_slice to Posix timestamp:
	df[,1] <- as.POSIXct(df[,1], tz="UTC")

	# Produce 16:9 PNG file in output
	p <- ggplot(data=df, aes(x=time_slice, y=count, group=lock_type, colour=lock_type)) +
		  geom_step() +
		  ylab("Lock Attempts Count") +
		  theme_minimal() +
		  theme(axis.title.x=element_blank(), legend.position="bottom",
				legend.direction="horizontal", legend.title=element_blank())
    ggsave("${PNAME}_locks.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "07 - Producing Lock Attempts graph... "
${RSCRIPT} ./locks.R >./locks.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./locks.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_locks.png]" ; }
test ${CLEAN} = true && rm -f ./locks.R ./locks.R.out ./${PNAME}_locks.txt

#---------------------------------------------------------------------------
# Step 8: Query Statistics Graphs:
#---------------------------------------------------------------------------
echo -n "08 - Extracting Query Statistics data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 8c:/,/^>>> Step 8d:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==14) print
    }' > ${PNAME}_qcons.txt
echo -e "${DONE}"
echo -n "08 - Generating Query Statistics by RP Analysis R Script... "
resource_pool='$resource_pool' # Dealing with variable substitution here below...
cat > qcons.R <<-EOF
	#!${RSCRIPT}
    library(dplyr)
    library(ggplot2)
    library(scales)
    #library(grid)
    library(gridExtra)

    # Read input file extracted from sprof output:
    df <- read.table("${PNAME}_qcons.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")


    #--------------------------------------------------------------------------
    # QUERY COUNT BY RP:
    #--------------------------------------------------------------------------
    # Create new Data Frame:
    tot <- sum(df[,2])
    xlm <- 1.15 * max(df[,2])
    mydf <- df %>%
        select(resource_pool, count) %>%
        mutate(pct = paste(round(100*count/tot, digits=1), "%", sep="")) %>%
        arrange(count) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p1 <- ggplot(mydf, aes(x=resource_pool, y=count, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(aes(x=resource_pool, xend=resource_pool, y=0, yend=count), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Number of queries by Resource Pool")

    #--------------------------------------------------------------------------
    # CPU ELAPS BY RP:
    #--------------------------------------------------------------------------
    # Create new Data Frame:
    tot <- sum(df[,3])
    xlm <- 1.15 * max(df[,3])
    mydf <- df %>%
        select(resource_pool, tot_cpu_s) %>%
        mutate(pct = paste(round(100*tot_cpu_s/tot, digits=1), "%", sep="")) %>%
        arrange(tot_cpu_s) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p2 <- ggplot(mydf, aes(x=resource_pool, y=tot_cpu_s, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(aes(x=resource_pool, xend=resource_pool, y=0, yend=tot_cpu_s), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total CPU execution time (seconds) by Resource Pool")

    #--------------------------------------------------------------------------
    # Bytes Read BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,6])
    xlm <- 1.15 * max(df[,6])
    mydf <- df %>%
        select(resource_pool, mbytes_read) %>%
        mutate(pct = paste(round(100*mbytes_read/tot, digits=1), "%", sep="")) %>%
        arrange(mbytes_read) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p3 <- ggplot(mydf, aes(x=resource_pool, y=mbytes_read, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=mbytes_read), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Data Read (MB) by Resource Pool")

    #--------------------------------------------------------------------------
    # Bytes Written BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,7])
    xlm <- 1.15 * max(df[,7])
    mydf <- df %>%
        select(resource_pool, mbytes_written) %>%
        mutate(pct = paste(round(100*mbytes_written/tot, digits=1), "%", sep="")) %>%
        arrange(mbytes_written) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p4 <- ggplot(mydf, aes(x=resource_pool, y=mbytes_written, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=mbytes_written), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Data Written (MB) by Resource Pool")
        
        
    #ggsave("${PNAME}_qcons1.png", plot=grid.draw(rbind(cbind(ggplotGrob(p1), ggplotGrob(p3), size="last"), cbind(ggplotGrob(p2), ggplotGrob(p4), size="last"), size = "last")), dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
    ggsave("${PNAME}_qcons1.png", plot=grid.arrange(p1, p2, p3, p4, ncol=2), dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    #--------------------------------------------------------------------------
    # Net IN BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,4])
    xlm <- 1.15 * max(df[,4])
    mydf <- df %>%
        select(resource_pool, net_mb_in) %>%
        mutate(pct = paste(round(100*net_mb_in/tot, digits=1), "%", sep="")) %>%
        arrange(net_mb_in) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p5 <- ggplot(mydf, aes(x=resource_pool, y=net_mb_in, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=net_mb_in), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Network Input (MB) by Resource Pool")

    #--------------------------------------------------------------------------
    # Net OUT BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,5])
    xlm <- 1.15 * max(df[,5])
    mydf <- df %>%
        select(resource_pool, net_mb_out) %>%
        mutate(pct = paste(round(100*net_mb_out/tot, digits=1), "%", sep="")) %>%
        arrange(net_mb_out) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p6 <- ggplot(mydf, aes(x=resource_pool, y=net_mb_out, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=net_mb_out), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Network Output (MB) by Resource Pool")

    #--------------------------------------------------------------------------
    # Bytes Loaded BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,8])
    xlm <- 1.15 * max(df[,8])
    mydf <- df %>%
        select(resource_pool, mbytes_loaded) %>%
        mutate(pct = paste(round(100*mbytes_loaded/tot, digits=1), "%", sep="")) %>%
        arrange(mbytes_loaded) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p7 <- ggplot(mydf, aes(x=resource_pool, y=mbytes_loaded, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=mbytes_loaded), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Data Load (MB) by Resource Pool")

    #--------------------------------------------------------------------------
    # Bytes spilled BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,9])
    xlm <- 1.15 * max(df[,9])
    mydf <- df %>%
        select(resource_pool, mbytes_spilled) %>%
        mutate(pct = paste(round(100*mbytes_spilled/tot, digits=1), "%", sep="")) %>%
        arrange(mbytes_spilled) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p8 <- ggplot(mydf, aes(x=resource_pool, y=mbytes_spilled, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=mbytes_spilled), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Data Spilled (MB) by Resource Pool")
    #ggsave("${PNAME}_qcons2.png", plot=grid.draw(rbind(cbind(ggplotGrob(p5), ggplotGrob(p7), size="last"), cbind(ggplotGrob(p6), ggplotGrob(p8), size="last"), size = "last")), dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
    ggsave("${PNAME}_qcons2.png", plot=grid.arrange(p5, p6, p7, p8, ncol=2), dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
    
    

    #--------------------------------------------------------------------------
    # Input ROWS BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,10])
    xlm <- 1.15 * max(df[,10])
    mydf <- df %>%  select(resource_pool, mrows_in) %>%   mutate(pct = paste(round(100*mrows_in/tot, digits=1), "%", sep="")) %>%  arrange(mrows_in) %>%  mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p <- ggplot(mydf, aes(x=resource_pool, y=mrows_in, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=mrows_in), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Rows Read by Resource Pool")
    ggsave("${PNAME}_qcons_mrows.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    #--------------------------------------------------------------------------
    # Processed ROWS BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,11])
    xlm <- 1.15 * max(df[,11])
    mydf <- df %>%
        select(resource_pool, mrows_proc) %>%
        mutate(pct = paste(round(100*mrows_proc/tot, digits=1), "%", sep="")) %>%
        arrange(mrows_proc) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p <- ggplot(mydf, aes(x=resource_pool, y=mrows_proc, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=mrows_proc), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Rows Processed by Resource Pool")
    ggsave("${PNAME}_qcons_mproc.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    #--------------------------------------------------------------------------
    # THREAD COUNT BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,13])
    xlm <- 1.15 * max(df[,13])
    mydf <- df %>%
        select(resource_pool, tot_thread_count) %>%
        mutate(pct = paste(round(100*tot_thread_count/tot, digits=1), "%", sep="")) %>%
        arrange(tot_thread_count) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p <- ggplot(mydf, aes(x=resource_pool, y=tot_thread_count, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=tot_thread_count), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Thread Count by Resource Pool")
    ggsave("${PNAME}_qcons_threads.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    #--------------------------------------------------------------------------
    # TOTAL ELAPSED BY RP:
    #--------------------------------------------------------------------------
    # Create New Data Frame
    tot <- sum(df[,14])
    xlm <- 1.15 * max(df[,14])
    mydf <- df %>%
        select(resource_pool, tot_duration_s) %>%
        mutate(pct = paste(round(100*tot_duration_s/tot, digits=1), "%", sep="")) %>%
        arrange(tot_duration_s) %>%
        mutate(resource_pool = factor(resource_pool, levels=.$resource_pool))

    # Plot:
    p <- ggplot(mydf, aes(x=resource_pool, y=tot_duration_s, label=pct)) +
        scale_y_continuous(limits=c(0, xlm)) +
        geom_segment(
            aes(x=resource_pool, xend=resource_pool, 
                y=0, yend=tot_duration_s), color="grey",size=2) +
        geom_point(size=3, color="#69b3a2") +
        geom_text(hjust=-0.5, vjust=0.5, size=3 ) +
        coord_flip() +
        theme_minimal() +
        theme(
            panel.grid.minor.y = element_blank(),
            panel.grid.major.y = element_blank(),
            legend.position="none",
			axis.text.y=element_text(size=12,face="bold"),
			axis.title=element_text(size=12,face="bold")) +
        xlab("") +
        ylab("Total Queries Elapsed Time (seconds) by Resource Pool")
    ggsave("${PNAME}_qcons_elaps.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")


    
EOF
echo -e "${DONE}"
echo -n "08 - Producing Query Statistics by RP graphs... "
${RSCRIPT} ./qcons.R >./qcons.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./qcons.R.out" ; } || \
    { echo -e "${DONE} [${PNAME}_qcons{count,cpu,netin,netout,mbread,mbspill,mrows,mproc,threads,elaps}.png]" ; }
test ${CLEAN} = true && rm -f ./qcons.R ./qcons.R.out ./${PNAME}_qcons.txt

#---------------------------------------------------------------------------
# Step 9: Number of projections by table distribution
#---------------------------------------------------------------------------
echo -n "09 - Extracting Projections by Table Distribution data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7m:/,/^>>> Step 7n:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==2) print
    }' > ${PNAME}_pbt.txt
echo -e "${DONE}"
echo -n "09 - Generating Projection by Table Distribution Analysis R Script... "
cat > pbt.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_pbt.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Plot:
	p <- ggplot(df, aes(x=n, y=count)) +
	    geom_bar(stat="identity", color="blue", fill="lightblue", width=1) + 
		scale_x_continuous(breaks=c(1:max(df[,1]))) + 
		xlab("") + ylab("") + 
		ggtitle("Projections by table distribution") + 
		theme_minimal()
    ggsave("${PNAME}_pbt.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "09 - Producing Projections by Table distribution graph... "
${RSCRIPT} ./pbt.R >./pbt.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./pbt.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_pbt.png]" ; }
test ${CLEAN} = true && rm -f ./pbt.R ./pbt.R.out ./${PNAME}_pbt.txt

#---------------------------------------------------------------------------
# Step 10: SELECT elapsed distribution
#---------------------------------------------------------------------------
echo -n "10 - Extracting SELECT Elapsed Distribution data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 9b:/,/^>>> Step 10:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==3) print
    }' > ${PNAME}_qed.txt
echo -e "${DONE}"
echo -n "10 - Generating SELECT Elapsed  Distribution Analysis R Script... "
cat > qed.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_qed.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Plot:
	p <- ggplot(df, aes(x=bucket, y=count, group=pool_name, colour=pool_name)) +
	    geom_point(size=3) + ggtitle("SELECT Elapsed distribution per resource pool") + 
		theme(legend.position="bottom")
    ggsave("${PNAME}_qed.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "10 - Producing SELECT Elapsed distribution graph... "
${RSCRIPT} ./qed.R >./qed.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./qed.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_qed.png]" ; }
test ${CLEAN} = true && rm -f ./qed.R ./qed.R.out ./${PNAME}_qed.txt

#---------------------------------------------------------------------------
# Step 11: Process Info
#---------------------------------------------------------------------------
echo -n "11 - Extracting Process Info... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 13c:/,/^>>> Step 14a:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==19) print
    }' > ${PNAME}_pinfo.txt
echo -e "${DONE}"
echo -n "11 - Generating Process Info R Scripts... "
cat > pinfo.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)
	library(scales)

	# Read input file extracted from sprof output:
    #   node_name                1  1 character
    #   start_time               2  2 POSIXct
    #   process                  3  - NULL
    #   address_space_max        4  - NULL
    #   data_size_max            5  - NULL
    #   open_files_max           6  - NULL
    #   threads_max              7  - NULL
    #   files_open_max_value     8  3 numeric
    #   sockets_open_max_value   9  4 numeric
    #   other_open_max_value    10  - NULL
    #   virtual_size_max_value  11  - NULL
    #   resident_size_max_value 12  5 numeric
    #   shared_size_max_value   13  - NULL
    #   text_size_max_value     14  - NULL
    #   data_size_max_value     15  - NULL
    #   library_size_max_value  16  - NULL
    #   dirty_size_max_value    17  - NULL
    #   thread_count_max_value  18  6 numeric
    #   map_count_max_value     19  - NULL
	df <- read.table("${PNAME}_pinfo.txt", sep="|", header=TRUE, na.strings="NULL",
                        colClasses=c("character","POSIXct","NULL","NULL","NULL","NULL","NULL",
                                     "numeric","numeric","NULL","NULL","numeric","NULL",
                                     "NULL","NULL","NULL","NULL","numeric","NULL"))

	# Node names limited to the last four characters
	df[,1] <- substring(df[,1],nchar(df[1,1])-3,nchar(df[1,1]))

    # Plot MAX open files
	tval <- 1000+round(max(df[,3]))
	p <- ggplot(data=df, aes(x=start_time, y=files_open_max_value, group=node_name, colour=node_name)) +
		geom_line() +
		ggtitle("${PNAME} Max opened files by hour") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=10000), minor_breaks=1:10)
    ggsave("${PNAME}_pinfo_files.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    # Plot MAX open sockets
	tval <- 100+round(max(df[,4]))
	p <- ggplot(data=df, aes(x=start_time, y=sockets_open_max_value, group=node_name, colour=node_name)) +
		geom_line() +
		ggtitle("${PNAME} Max opened sockets by hour") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=100), minor_breaks=1:10)
    ggsave("${PNAME}_pinfo_sockets.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    # Plot MAX RSS
    df[,5] <- df[,5]/(1024*1024*1024)
	tval <- 10+round(max(df[,5]))
	p <- ggplot(data=df, aes(x=start_time, y=resident_size_max_value, group=node_name, colour=node_name)) +
		geom_line() +
        ggtitle("${PNAME} Max Resident Set Size (RSS) GB by hour") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=10), minor_breaks=1:10)
    ggsave("${PNAME}_pinfo_rss.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    # Plot MAX thread Count by Hour
	tval <- 1000+round(max(df[,6]))
	p <- ggplot(data=df, aes(x=start_time, y=thread_count_max_value, group=node_name, colour=node_name)) +
		geom_line() +
        ggtitle("${PNAME} Max thread count by hour") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=2000), minor_breaks=1:10)
    ggsave("${PNAME}_pinfo_threads.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "11 - Producing Process Info graphs... "
${RSCRIPT} ./pinfo.R >./pinfo.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./pinfo.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_pinfo_files.png, ${PNAME}_pinfo_sockets.png, ${PNAME}_pinfo_rss.png, ${PNAME}_pinfo_threads.png]" ; }
test ${CLEAN} = true && rm -f ./pinfo.R ./pinfo.R.out ./${PNAME}_pinfo.txt

#---------------------------------------------------------------------------
# Step 12: Spread Retransmissions
#---------------------------------------------------------------------------
echo -n "12 - Extracting Spread Retransmissions Info... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 5c:/,/^>>> Step 6a:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==6) print
    }' | grep -v NULL > ${PNAME}_spread.txt
echo -e "${DONE}"
echo -n "12 - Generating Spread Retransmissions R Scripts... "
cat > spread.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)
	library(scales)

	# Read input file extracted from sprof output:
    #   time                    1  1 POSIXct
    #   node_name               2  2 character
    #   retrans                 3  3 numeric
    #   time_interval           4  - NULL
    #   packet_count            5  4 numeric
    #   retrans_per_second      6  5 numeric
	df <- read.table("${PNAME}_spread.txt", sep="|", header=TRUE, na.strings="NULL",
                        colClasses=c("POSIXct","character","numeric","NULL","numeric","numeric"))

	# Node names limited to the last four characters
	df[,2] <- substring(df[,2],nchar(df[1,2])-3,nchar(df[1,2]))

    # Plot retrans 
	tval <- 10+round(max(df[,3]))
	p <- ggplot(data=df, aes(x=time, y=retrans, group=node_name, colour=node_name)) +
		geom_line() +
		ggtitle("${PNAME} Spread Retrans") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=100), minor_breaks=seq(1,tval,10))
    ggsave("${PNAME}_spread_retrans.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    # Plot packet count
	tval <- 100+round(max(df[,4]))
	p <- ggplot(data=df, aes(x=time, y=packet_count, group=node_name, colour=node_name)) +
		geom_line() +
		ggtitle("${PNAME} Spread retrans packet count") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=1000), minor_breaks=seq(1,tval,100))
    ggsave("${PNAME}_spread_pcount.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

    # Plot retrans % per second
	tval <- 110
	p <- ggplot(data=df, aes(x=time, y=retrans_per_second, group=node_name, colour=node_name)) +
		geom_line() +
        ggtitle("${PNAME} Spread Retrans % per second") +
		xlab("") + ylab("") +
		ylim(0, tval) +
		theme(axis.text.x=element_text(angle=90,hjust=1),
			  panel.background=element_blank(),
			  panel.grid.major=element_line(colour="dimgray"),
			  panel.grid.minor=element_line(colour="lightgray"),
			  legend.background=element_blank(),
			  legend.title=element_blank()) +
		scale_x_datetime(labels=date_format("%d-%m", tz="UTC")) +
		scale_y_continuous(breaks=seq(0, tval, by=10), minor_breaks=seq(1,tval,1))
    ggsave("${PNAME}_spread_rps.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "12 - Producing Spread Retrans graphs... "
${RSCRIPT} ./spread.R >./spread.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./spread.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_spread_retrans.png, ${PNAME}_spread_pcount.png, ${PNAME}_spread_rps.png]" ; }
test ${CLEAN} = true && rm -f ./spread.R ./spread.R.out ./${PNAME}_spread.txt


#---------------------------------------------------------------------------
# Step 13: Catalog Size Per Node
#---------------------------------------------------------------------------
echo -n "13 - Extracting Catalog Size... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7j:/,/^>>> Step 7k:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==3) print
    }' > ${PNAME}_catalog.txt
echo -e "${DONE}"
echo -n "13 - Generating Catalog Size Per Node Analysis R Script... "
cat > catalog.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_catalog.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Node names limited to the last four characters
	df[,2] <- substring(df[,2],nchar(df[1,2])-3,nchar(df[1,2]))

	# Plot:
	p <- ggplot(df, aes(x=node_name, y=END_CATLOG_SIZE_MEM_MB)) +
	    geom_bar(stat="identity", color="blue", fill="lightblue", width=1) + 
		xlab("") + ylab("") + 
		ggtitle("Catalog Size Per Node") + 
		theme(axis.text.x = element_text(angle = 90, hjust = 1))
    ggsave("${PNAME}_catalog.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "13 - Producing Catalog Size Per Node graph... "
${RSCRIPT} ./catalog.R >./catalog.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./catalog.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_catalog.png]" ; }
test ${CLEAN} = true && rm -f ./catalog.R ./catalog.R.out ./${PNAME}_catalog.txt


##---------------------------------------------------------------------------
# Step 14: Storage Containers
#---------------------------------------------------------------------------
echo -n "14 - Storage Containers ... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7r:/,/^>>> Step 7s:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==2) print
    }' > ${PNAME}_containers.txt
echo -e "${DONE}"
echo -n "14 - Generating Number of Storage containers Analysis R Script... "
cat > containers.R <<-EOF
	#!${RSCRIPT}
	library(ggplot2)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_containers.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Node names limited to the last four characters
	df[,1] <- substring(df[,1],nchar(df[1,1])-3,nchar(df[1,1]))

	# Plot:
	p <- ggplot(df, aes(x=node_name, y=num_storage_containers)) +
	    geom_bar(stat="identity", color="blue", fill="lightblue", width=1) + 
		xlab("") + ylab("") + 
		ggtitle("Number of Storage Containers Per Node") + 
		theme(axis.text.x = element_text(angle = 90, hjust = 1))
    ggsave("${PNAME}_containers.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")
EOF
echo -e "${DONE}"
echo -n "14 - Producing Number of Storage Containers Per Node graph... "
${RSCRIPT} ./containers.R >./containers.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./containers.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_containers.png]" ; }
test ${CLEAN} = true && rm -f ./containers.R ./containers.R.out ./${PNAME}_containers.txt


#---------------------------------------------------------------------------
# Step 15: Delete Vectors
#---------------------------------------------------------------------------
echo -n "15 - Extracting Delete Vectors Data... "
${CAT} ${SPOUT} | awk -F\| '/^>>> Step 7o:/,/^>>> Step 7p:/{
		sub(/^ */,"");
		gsub(/ *\| */,"|",$0);
        if(NF==5) print
    }' > ${PNAME}_dv.txt
echo -e "${DONE}"

echo -n "15 - Generating Delete Vectors R Script... "
cat > dv.R <<-EOF
	#!${RSCRIPT}

	# Load libraries
	library(ggplot2)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_dv.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	# Node names limited to the last four characters
	df[,1] <- substring(df[,1],nchar(df[1,1])-3,nchar(df[1,1]))

	# Scaling Factor to have 2 axis in the graph
	sf <- max(df\$num_del_vectors)/max(df\$num_del_rows)
	# Plot
	p <- ggplot(df) +geom_bar(mapping = aes(x=node_name, y=num_del_vectors, group=storage_type, colour=storage_type), stat="identity" ,color="blue",fill="lightblue")  +  geom_line(mapping = aes(x=node_name, y=num_del_rows*sf, group=storage_type, colour=storage_type),size=2)  +scale_y_continuous(name="Delete Vectors", sec.axis=sec_axis(~ ./sf/1000,name="Rows Deleted (Thousands) ", labels=function(n){format(n, scientific=FALSE)}))  +theme(axis.text.x = element_text(angle = 90, hjust = 1))  +ggtitle("Delete Vector Count vs Deleted Rows")

    ggsave("${PNAME}_dv.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

EOF
echo -e "${DONE}"
echo -n "15 - Producing Delete Vectors graphs... "
${RSCRIPT} ./dv.R >./dv.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./dv.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_dv.png]" ; }
test ${CLEAN} = true && rm -f ./dv.R ./dv.R.out ./${PNAME}_dv.txt


#---------------------------------------------------------------------------
# Step 16: Connection Load balancing Graph  
#---------------------------------------------------------------------------


echo -n "16 - Connection Load balancing Graph R Script... "
cat > clb.R <<-EOF
	#!${RSCRIPT}

	# Load libraries
	library(ggplot2)

	# Read input file extracted from sprof output:
	df <- read.table("${PNAME}_connectionbalancing.txt", sep="|", header=TRUE, as.is=TRUE, na.strings="NULL")

	#loads_initiated, selects_initiated


	# Plot
	
  p<-ggplot(df) +
        geom_line(aes(x=node_num, y=loads_initiated, color="loads_initiated")) + geom_point(aes(x=node_num, y=loads_initiated, color="loads_initiated")) +
        geom_line(aes(x=node_num, y=selects_initiated, color="selects_initiated")) + geom_point(aes(x=node_num, y=selects_initiated, color="selects_initiated")) +
        scale_color_brewer(palette="Paired") +  
          ylab('Values') +
           scale_x_discrete(labels=seq(1,length(df\$node_num),1), limits=seq(1,length(df\$node_num),1), breaks=seq(1,length(df\$node_num),1) ) +
           theme_minimal() + theme(legend.position="right") 

    ggsave("${PNAME}_clb.png", plot=p, dpi=${PNGRES}, width=${PNGWIDTH}, height=${PNGHEIGHT}, units="in")

EOF
echo -e "${DONE}"
echo -n "16 - Producing connection load balancing Graph... "
${RSCRIPT} ./clb.R >./clb.R.out 2>&1
test $? -ne 0 && { echo -e "${FAIL} - check ./clb.R.out" ; } || \
	{ echo -e "${DONE} [${PNAME}_clb.png]" ; }
test ${CLEAN} = true && rm -f ./clb.R ./clb.R.out 



#---------------------------------------------------------------------------
# Step 17: Prepare PPTX
#---------------------------------------------------------------------------
echo -n "17 - Prepare Presentation "
cat > preparePPT.R <<-EOF
	#!${RSCRIPT}

	library(magrittr)
	library(flextable)
	library(officer)

   
  MAX_ROW_PER_SLIDE <- $MAXROWS
   
   
  projName="${PNAME}"
	finalDoc<-paste(projName,"_sprof_Analysis.pptx",sep="")
	doc <- read_pptx("${PPTEMPL}")
	

	
	## Flex Table formating constants
	def_cell <- fp_cell(border = fp_border(color="black"))
	def_par_form <- fp_par(text.align = "center")
	def_text <- fp_text(color="black", font.family = "Arial", font.size = 10)
	def_text_header <- update(color="white", def_text, bold = TRUE, font.family = "Arial", font.size = 10)
	


  #formating function 
  FitFlextableToPage <- function(ft, pgwidth = 6){
    ft_out <- ft %>% autofit()
    ft_out <- width(ft_out, width = dim(ft_out)\$widths*pgwidth /(flextable_dim(ft_out)\$widths))
    return(ft_out)
  }



#formating common function  
 apply_formatstyle_flextable <- function(dat, pgwidth = 6, bold_first_column = FALSE, cols=NULL, widths=NULL) {


  ft <- flextable::qflextable(dat)

  # Apply general style for text
  ft <- flextable::style( ft, pr_c = fp_cell(border = fp_border(color="black")), 
        pr_p = fp_par(text.align = "center"), 
        pr_t = fp_text(color="black", font.family = "Arial", font.size = 8), 
        part = "all")  
  
  # Apply border line Styles
  def_big_b1 <- fp_border(color="black", width = 2)
	def_std_b1 <- fp_border(color="gray30", style = "dotted",  width = 1)
  
  
  ft <- vline( ft, border = def_std_b1, part = "all" )
  ft <- vline_left( ft, border = def_big_b1, part = "all" )
  ft <- vline_right( ft, border = def_big_b1, part = "all" )
  ft <- hline( ft, border = def_std_b1 )
  ft <- hline_bottom( ft, border = def_big_b1 )
  ft <- hline_top( ft, border = def_big_b1, part = "all") 
  
  
  # Apply header styling
  ft <- flextable::bold(ft, j = 1, part = "header")
  # Set background color for header row
  ft <- flextable::bg(ft, bg = "#037AEE", part = "header")
  ft <- flextable::style( ft, 
        pr_t = fp_text(color="white", font.family = "Arial", font.size = 10), 
        part = "header")  
        
  ft <- bold(ft, bold = TRUE, part = "header")

  # Make first column bolded
  if (bold_first_column) {
    ft <- flextable::bold(ft, j = 1, part = "all")
  }

  #adjust column widths
  if (length(cols) > 0 && length(widths) > 0) {
      # Check if vectors are of equal length
      if (length(cols) != length(widths)) {
        stop("Vectors must be of equal length.")
      }
      
     ft <- width(ft, j = cols, width = widths)
   }
 
  # fit to page
  ft <- FitFlextableToPage(ft, pgwidth)
  
 
  return(ft)
}




# Function to add a slide with a flextable
 add_flextable_slide <- function(doc, data, mystitle = "Table Slide tittle",  pgwidth = 8,  bold_first_column = FALSE, cols=NULL, widths=NULL) {
  doc <- add_slide(doc, layout = "Basic Content", master = "Office Theme")
  doc <- ph_with(x = doc, value = mystitle, location = ph_location_type(type = "title"))
  
  ft <- apply_formatstyle_flextable(data, pgwidth,  bold_first_column, cols, widths )
  
  doc <- ph_with(x = doc, value = ft, location = ph_location_type(type = "body"))
  return(doc)
 }	
 
 
### Start presentation building .... 
 
 
	# Set the tittle Slide. 
	stitle <- "Tittle Slide "
	doc <- add_slide(doc, layout = "Tittle Slide",  master = "Office Theme")
	doc <- ph_with(x = doc, value = "Cluster Health Check: ${PNAME}" , location = ph_location_label(ph_label="Title 1"))
	doc <- ph_with(x = doc, value = paste("[Author]", format(Sys.Date( ) , format="%d %B %Y"),sep ="\n") , location = ph_location_label(ph_label="Subtitle 2"))
	
	
		
	# Analysis Information. Steps: 0a, 1a, 15
	stitle <- "Analysis Information "
	write(stitle, stdout())
	try(
		{
			
      file <- paste(projName,"_sprof.txt", sep="")
      dat <- read.csv(file, header=TRUE, sep="|")

      # add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = 8,  bold_first_column = TRUE)
		}
	)

  # Collector Data
	stitle <- "Data Collector retention Information "
	write(stitle, stdout())
	try(
		{
			file <- paste(projName,"_DC.txt", sep="")
			dat <- read.csv(file, header=TRUE, sep="|")
      
			# add slide
			doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE, cols=c(1), widths = c(2)) 
      
		}
	)



	# General Information. Steps: 6a, 1c
	stitle <- "General Information "
	write(stitle, stdout())
	try(
		{
			file <- paste(projName,"_Info.txt", sep="")
			dat <- read.csv(file, header=TRUE, sep=":")

      #add slide   
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = 6,  bold_first_column = TRUE)
			
			
		}
	)

	# System Information. Steps: 1b
	stitle <- "System Information "
	write(stitle, stdout())
	try(
		{
			file <- paste(projName,"_System.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|", colClasses="character")

      # add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH/2,  bold_first_column = TRUE)
		}
	)

	# Non Default Parameters. Steps: 3
	stitle <- "Non-default Configuration Parameters "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName, "_ConfParam.txt" ,sep="")
			dat <- read.csv(file, header = TRUE, sep="|")
      
      # add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE, cols=c(2,3), widths = c(1.5,1.5))
			
		}
	)

	# Cluster Config. Step: 5a
	stitle <- "Cluster Configuration"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_Host.txt", sep="")
			dat <- read.csv(file, header = TRUE, sep="|")
      
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
      
		}
	)

	# Resource Pool Config. Steps: 4
	stitle <- "Resource Pools configuration"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName, "_RP.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")


      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
      
      
		}
	)

	# Common Data Types. Steps: 7a
	stitle <- "Common Data Types"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_ColumnType.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")
		
   
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

    }
	)

	# Large Schemas. Steps: 7b
	stitle <- "Largest Schemas"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName, "_LargeSchema.txt", sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")
	
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
 
 
		}
	)
	   
	# Tables with more columns. Steps: 7c
	stitle <- "Tables with more columns"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_TableColumns.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }



		}
	)

	# Tables with large rows. Steps: 7d
	stitle <- "Tables with largest rows"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_TableRows.txt",sep="")
			dat <-read.csv(file, header = TRUE, sep = "|")
      
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)

	# Largest Segmented Projections. Steps: 7e
	stitle <- "Largest Segmented Projections"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName, "_SegProj.txt", sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }


		}
	)

	# Largest Unsegmented projections. Steps: 7f
	stitle <- "Largest Unsegmented Projections"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName, "_RepProj.txt", sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }


		}
	)

	# Less used projections. Steps: 7h
	stitle <- "Top 10 less used projections "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_UsedProj.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
    }
	)

	# Catalog Size. Steps: 7j
	stitle <- "Catalog Size by node"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_catalog.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file.path(getwd(),file), width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)
	  
	# Projections Basename Distribution. Steps: 7m
	stitle <- "Projection Basename Distribution"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_BaseName.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")	
      
      # add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH/3,  bold_first_column = TRUE)


		}
	)
	  
	# Projection Basename Distribution Graph. Steps: 7m
	stitle <- "Projection Basenames per Table"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_pbt.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)
	   
	# Projection Basenames by Creation Type: 7s
	stitle <- "Projection Basenames by Creation Type"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_ProjCreationType.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")	
      
      # add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH/2,  bold_first_column = TRUE)
		}
	)

	# Delete Vectors & rows Deleted by node. Steps: 7o
	stitle <- "Delete Vectors and deleted rows by node"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_dv.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)
	  
	# Number of Objects. Steps: 7k, 7l, 7n
	stitle <- "Number of objects"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_Objects.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = ":")	
      
      # add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH/2,  bold_first_column = TRUE)
		}
	)
	  
	# Column Statistics. Steps: 7p
	stitle <- "Columns statistics"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_ColumnStats.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")	
      
			# add slide
      doc <- add_flextable_slide(doc, dat, stitle, pgwidth = $PNGWIDTH/2,  bold_first_column = TRUE)
		}
	)
	  
	# Column Encodings. Steps: 7q
	stitle <- "Columns Encodings"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_ColumnEnc.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")	
			
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
      
		}
	)
	  
	# Number of storage containers by node. Steps: 7r
	stitle <- "Number of storage containers"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_containers.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)
	 
	# Workload by Hour and Request Type. Steps: 8b
	stitle <- "Workload by Hour and request_type "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_wload.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
      
 		}
	)

	# Workload by Hour and Query Type. Steps: 8b
	stitle <- "Workload by Hour (request_type=QUERY) "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_wload2.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file.path(getwd(),file),width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
      
		}
	)

	# Query Elapsed Distribution Overview. Steps: 9a
	stitle <- "Query Elapsed distribution overview"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_QueryET.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")	

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)
	   
	# Select Elapsed Distribution by RP. Steps: 9b
	stitle <- "SELECT Elapsed Distribution "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_qed.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Statement Execution Percentile
	stitle <- "Statements Execution percentile"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_QueryEP.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 
      
      
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
		}
	)

	# Query Consumptions 1. : 8c
	stitle <- "Query Consumptions by Resource Pool (1)"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_qcons1.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Query Consumptions 2. : 8c
	stitle <- "Query Consumptions by Resource Pool (2)"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_qcons2.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Query Events by Request Type : 8d
	stitle <- "Query Events by Request Type"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_QERequest.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)

	# Query Events by Statement Type : 8e
	stitle <- "Query Events by Statement Type"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_QEStatement.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)

	# Query Concurrency. Steps: 10
	stitle <- "Query Concurrency"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_concurrency.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file.path(getwd(),file), width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	### Added by MP
	# Queue Waiting Time. Step: 10b
	stitle <- "Queue Waiting Time per resource pool"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_QueueWait.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)


	# Lock Attempts Overview. Steps: 12a
	stitle <- "Lock Attempts Overview"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_LockAttemp.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|", stringsAsFactors=FALSE) 
      
      ### Trim 4th column (description) to first 64 chars
      max_length <- 64
       dat[, 4] <- substr(dat[, 4], 1, max_length)
      	

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE, cols = c(1,2,3,4,5), widths = c(2.5,1,2,4.5,1))
        step<-step+1
      }


		}
	)

	# Lock Attempts Count. Steps: 12b
	stitle <- "Lock Attempts Overview "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_locks.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# CPU idle by node. Steps: 13a
	stitle <- "CPU Idle "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_cpu_idle.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Free memby node. Steps: 13b
	stitle <- "Free Memory   "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_free_mem.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Pinfo Files. Steps: 13c
	stitle <- "Process Info - Files Open  "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_pinfo_files.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Pinfo Sockets. Steps: 13c
	stitle <- "Process Info - Socket Open  "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_pinfo_sockets.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Pinfo RSS. Steps: 13c
	stitle <- "Process Info - RSS Memory "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_pinfo_rss.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Pinfo Threads Steps: 13c
	stitle <- "Process Info - Threads Count "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_pinfo_threads.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Spread Packet Count. Steps: 5c
	stitle <- "Spread Packet Count  "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_spread_pcount.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Spread Retransmit. Steps: 5c
	stitle <- "Spread Retransmit  "
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_spread_retrans.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# Spread Retransmit per second. Steps: 5c
	stitle <- "Spread Retransmit per second"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_spread_rps.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# TM Events count and Size. Steps: 14a
	stitle <- "TM Event Count/Size by hour"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_tm.png",sep="")
			doc <- add_slide(doc, layout = "Basic Content",  master = "Office Theme")
			doc <- ph_with(x = doc, value = stitle , location = ph_location_type(type="title") )
			doc <- ph_with(x = doc, external_img(file,width = ${PNGWIDTH}, height = ${PNGHEIGHT}), location = ph_location_left(), use_loc_size = FALSE )
		}
	)

	# TM Events Duration. Steps: 14b
	stitle <- "TM durations"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_TMDurations.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 	

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }


		}
	)

	# TM Longest Mergeout. Steps: 14c
	stitle <- "Tuple Mover Mergeout (taking more than 20 mins)"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_TMLongM.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 	

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
      
		}
	)

	# TM Longest Replay Delete. Steps: 14rdc
	stitle <- "TM Replay Deletes taking more than 10 mins"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_TMLongRD.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 	

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
      
		}
	)
	  
	# Slow Events. Steps: 5d
	stitle <- "Slow Events"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_SlowEvent.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
    }
	)

  # Other information 
	# LockAttempts (VAdvisor format). Steps: 15a
	stitle <- "LockAttempts (VAdvisor format)"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_LockAttemptsVA.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)

	# LockHolds Stats (VAdvisor format). Steps: 15b
	stitle <- "LockHolds Stats (VAdvisor format)"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_LockHoldsVA.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
		}
	)

	# Transaction with GCLX. Steps: 15c
	stitle <- "Transaction with GCLX"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_TXGCLX.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)

	# Projection with data SKEW. Steps: 15d
	stitle <- "Projection with data SKEW"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_PrjSkewness.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)

	# Disks Percent utilization. Steps: 15e
	stitle <- "Disks Percent utilization"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_DskPctFull.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }

		}
	)


	# Queries using more than 25 percent of memory. Steps: 15f
	stitle <- "Queries using more than 25 percent of memory"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_Over25pctMem.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|") 

      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
   }
	)


	# Projections with ROS containers above 256. Steps: 15g
	stitle <- "Projections with ROS containers above 256"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_ROS256.txt",sep="")
			dat <- read.csv(file, header = TRUE, sep = "|")       


      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
        step<-step+1
      }
      
		}
	)


	#Load Streams: 15i
	stitle <- "Load Streams Stats"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_loadstreams.txt",sep="")
      filepng<-paste(projName,"_clb.png",sep="")
      
      
      dat <- read.csv(file, header = TRUE, sep = "|")
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH,  bold_first_column = TRUE)
               
        step<-step+1
      }
      
      
      
      
			
		}
	)


	#Connections Initiated per node. Steps: 15h
	stitle <- "Connections Initiated per node"
	write(stitle, stdout())
	try(
		{
			file<-paste(projName,"_connectionbalancing.txt",sep="")
      filepng<-paste(projName,"_clb.png",sep="")
      
      
      dat <- read.csv(file, header = TRUE, sep = "|")
      # Split the data into chunks based on MAX_ROW_PER_SLIDE
      rows_per_chunk <- ceiling(nrow(dat) / MAX_ROW_PER_SLIDE)
      chunks <- split(dat, rep(seq_len(rows_per_chunk), each = MAX_ROW_PER_SLIDE, length.out = nrow(dat)))

      step <- 1
      # Add slides for each chunk
      for (chunk in chunks) {
        if (rows_per_chunk > 1) 
            cstitle <- sprintf("%s #%d/%d", stitle, step, rows_per_chunk)
        else  cstitle <- stitle
        doc <- add_flextable_slide(doc, chunk, cstitle, pgwidth = $PNGWIDTH/2,  bold_first_column = TRUE)
        doc <- ph_with(x = doc, external_img(filepng, width = ${PNGWIDTH/2}, height = ${PNGHEIGHT}), location = ph_location_right(type="body"), use_loc_size = TRUE )
        
        step<-step+1
      }
      
      
      
      
			
		}
	)


	# Thank You. 
	doc <- add_slide(doc, layout = "Thank You",  master = "Office Theme")
	doc <- ph_with(x = doc, value = "Thank You" , location = ph_location_label(ph_label="Title 1"))
	doc <- ph_with(x = doc, value = "www.vertica.com" , location = ph_location_left())

	# End:
	print(doc, target = finalDoc)

EOF


echo -e "${DONE}"
echo -n "17 - Save PPT "
${RSCRIPT} ./preparePPT.R >./preparePPT.R.out 2>&1
grep -qi error ./preparePPT.R.out && { echo -e "${WARN} - check ./preparePPT.R.out" ; } || \
	{ echo -e "${DONE}" ; }
test ${CLEAN} = true && rm -f \
    ./preparePPT.R \
    ${PNAME}_BaseName.txt \
    ${PNAME}_DC.txt \
    ${PNAME}_CatalogSize.txt \
    ${PNAME}_ColumnEnc.txt \
    ${PNAME}_ColumnStats.txt \
    ${PNAME}_ColumnType.txt \
    ${PNAME}_ConfParam.txt \
    ${PNAME}_Host.txt \
    ${PNAME}_Info.txt \
    ${PNAME}_LargeSchema.txt \
    ${PNAME}_LockAttemp.txt \
    ${PNAME}_NumDV.txt \
    ${PNAME}_Objects.txt \
    ${PNAME}_QueryEP.txt \
    ${PNAME}_QueryET.txt \
    ${PNAME}_RP.txt \
    ${PNAME}_RepProj.txt \
    ${PNAME}_SCont.txt \
    ${PNAME}_SegProj.txt \
    ${PNAME}_SizeSchema.txt \
    ${PNAME}_SlowEvent.txt \
    ${PNAME}_System.txt \
    ${PNAME}_TMDurations.txt \
    ${PNAME}_TMLongM.txt \
    ${PNAME}_TMLongRD.txt \
    ${PNAME}_TableColumns.txt \
    ${PNAME}_TableRows.txt \
    ${PNAME}_TotalSchema.txt \
    ${PNAME}_UsedProj.txt \
    ${PNAME}_sprof.txt \
	  ${PNAME}_QueueWait.txt \
	  ${PNAME}_ProjCreationType.txt \
	  ${PNAME}_QERequest.txt \
	  ${PNAME}_QEStatement.txt \
    ${PNAME}_LockAttemptsVA.txt \
    ${PNAME}_LockHoldsVA.txt \
    ${PNAME}_TXGCLX.txt \
    ${PNAME}_PrjSkewness.txt \
    ${PNAME}_DskPctFull.txt \
    ${PNAME}_Over25pctMem.txt \
    ${PNAME}_ROS256.txt \
    ${PNAME}_connectionbalancing.txt \
    ${PNAME}_loadstreams.txt \
    ${PNAME}_SizeTypes.txt \
    ${PNAME}_Compression.txt \
	  ${PNAME}_*.png
