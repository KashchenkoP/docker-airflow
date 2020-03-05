set -x #echo on

COUNTRY_PANJIVA_FOLDER=''

for archieve in "$COUNTRY_PANJIVA_FOLDER"/*.zip
do
        unzip $archieve
        # This block of code is used to provide a difference between different files 
        # with the same *panjiva*.txt name
        prefix=$(tr 'Full' '\n' <<< $archieve | tail -n1)
        prefix=$(tr '.' '\n' <<< $prefix | head -n1)
        panjiva_file=${prefix}_$(ls *.txt)
        mv *panjiva*.txt "$panjiva_file"

        # Fix the escape char and remove useless ' symbols
        perl -p -e 's/\n/ /g' < "$panjiva_file" > /data/tmp
        perl -lne 'BEGIN{$/="#@#@#"} print "$_"' < /data/tmp > "$panjiva_file"
        rm /data/tmp
        sed -i "s/'//g" "$panjiva_file"
        hadoop fs -put /data/"$panjiva_file" /data-lake/raw-data-zone/rec/panjiva/ftp/"$COUNTRY_PANJIVA_FOLDER"/
        rm "$panjiva_file"
        rm $archieve
done
