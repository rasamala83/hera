overall=0
export CACHE_HOST="localhost"
export CACHE_CERTS_PATH=""
for d in `ls -F tests/functionaltest/caching_tests | grep /$ | sed -e "s,/,,"`
do 
    echo ==== $d
    # pushd tests/unittest/$d 
    # cp /home/runner/go/bin/mysqlworker .
    rm -f *.log 
    $GOROOT/bin/go test -c github.com/paypal/hera/tests/functionaltest/caching_tests/$d
    ./$d.test -test.v
    rv=$?
    grep -E '(FAIL|PASS)' -A1 *.log
    if [ 0 != $rv ]
    then
        echo "Retrying" $d
        echo "exit code" $rv 
        ./$d.test -test.v
        rv=$?
        grep -B5 -A5 -E '(FAIL|PASS)' -A1 *.log
    fi
    if [ 0 != $rv ]
    then
        #grep ^ *.log
        # popd
        cp hera.log hera.log.$d
        #exit $rv
        overall=1
        continue
    fi
    rm -f *.log 
    # popd
done
exit $overall
