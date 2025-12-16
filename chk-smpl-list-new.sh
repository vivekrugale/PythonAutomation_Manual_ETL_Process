if [ $# -ne 1 ]
then
   echo "Syntax error: chk-smpl-list-new.sh <Input file>"
   exit 1
fi
ifile=$1
if [ ! -f $ifile ]
then
   echo "Error: Missing input file $ifile"
   exit 1
fi
for mid in `cat $ifile`
do      
  
  mkt=$(echo $mid | cut -f1 -d';')
  sm=$(echo $mid | cut -f2 -d';')
  fnm=$(grep -r -l -i $sm /iics_pmroot/ARCHIVE/SMP_ZP_OUT_IICS/$mkt/*.csv | sort -r | head -1 | cut -f6 -d'/' )
  if [ -n "$fnm" ]
  then          
     echo "$sm:$fnm"
  else          
     echo "$sm:nf" 
  fi 
done
