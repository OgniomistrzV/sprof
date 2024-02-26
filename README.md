# sprof
Vertica sproff script collecting the data about system + preprocessor that generates the PowerPoint slide deck out of it.

## Running SPROF

To collect the data run the ./sprof-0.5.sh at customer site. 
Different option can be seen with –h flag

`Usage: sprof [-o | --output out_file] [-g |--gzip] [-c schema] [-m schema] [-i schema] [-S start] [-E end] [-h|--help]`<br>
`-o | --output out_file: defines output file (default sprof.out)`
`-g | --gzip: gzips output file)`
`-m schema: defines monitoring schema (default v_monitor)`
`-i schema: defines dc_tables schema (default v_internal)`
`-c schema: defines catalog schema (default v_catalog)`
`-S YYYY-MM-DD: defines start date (default 0001-01-01)`
`-E YYYY-MM-DD: defines end date (default 9999-12-31)`
`-h | --help: prints this message`



Once that you have the sprof output:

1. Create a folder in this server, folder ./sprof/users-sprof
2. Copy the output file to that folder 
3. Process the data with the Post Processor **sprofpp-0.5a.sh** that match the version used to collect the data


**sprofPP-0.5a.sh** options can be seen with *** –h flag***. 



`Usage: sprofpp [-p project_name] [-W width] [-H height] [-R res] [-h] [-c] [-t template PPTX file] [--help] -f sprof_output_file`
`-p project_name to set the project name (default \'project\')`
`-W width PNG Graphs Width (default 11.61)`
`-H height PNG Graphs Height (default 4.83)`
`-R resolution PNG Graphs Resolution (default 96)`
`-a {none|secs|mins|hours} concurrency aggregation level (default hours)`
`-c clean temporary files`
`-t location to a PPTX template file based on which PPTX is generated. Default location: 'bin/templates/master.pptx'`
`-h | --help print this message`




##  Process SPROF results and prepare PowerPoint deck automatically

### 1) Install the Environment
   sprofpp uses R to generate graphs and build PPT so you will need: 
**   A) Unpack keeping the structure**
       
**   B) Download & Install R for Linux**:
   
      -  * UBUNTU:
              https://linuxize.com/post/how-to-install-r-on-ubuntu-20-04/
       
      -  * CENTOS/REDHAT
               https://linuxize.com/post/how-to-install-r-on-centos-8/
       
     	you may need also a compiler...See:
         UBUNTU: sudo apt install build-essential
         RH/CENTOS: sudo yum install build-essential
         

**        C) Install required libraries**
       - * sudo yum|apt install cairo-devel 
	   - * sudo yum|apt install libxml2-devel

**   D) If you had R previously installed:**

       # update existing packages
       update.packages(ask = FALSE)
       install.packages("devtools")

       
   **E) After you did install R, start it and install the following additional packages (in R):**
   
```r
install.packages("ggplot2")   # This is to prepare graphs…
install.packages("dplyr")     # This is to manipulate data
install.packages("scales") 
install.packages("magrittr")
install.packages("officer")   # This is to produce PowerPoint
install.packages("grid")
install.packages("gridExtra")
install.packages("flextable")     # Thus is to format tables
```
   
        
You are all set. You can now produce PPTs like the attached one automatically on your Mac. 
All you might need at the end is to adjust fonts in order to fit tables in one slide or so…

You can play a little with the sprof post-processor options (-h= help). attached powerpoint:

```bash
/sprofpp-0.5.sh -p Project_Name -a none -c -f ../sprof-user/sprof_output.out
```


For completeness sake I also attach **sprof-0.5a.sh** (remember to always use the corresponding version of the Post-Processor).
