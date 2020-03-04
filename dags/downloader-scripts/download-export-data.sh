for dir in Panjiva*Export{2011...2020}
do
        echo "$dir"
        #wget --user username --password pwd -r -nd -np -l1 -A '*.zip' ftp://edx.standardandpoors.com/Products/$dir/
done