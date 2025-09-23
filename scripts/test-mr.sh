#!/usr/bin/env bash

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race
# 获取脚本自身的绝对路径
SCRIPT_PATH="$(realpath "$0")"

# 脚本所在目录
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"

# 脚本的上一层目录作为根目录
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${ROOT_DIR}/build"
if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}


TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
rm -rf ${BUILD_DIR}
mkdir ${BUILD_DIR} || exit 1
cd ${BUILD_DIR} || exit 1
rm -f mr-*
cp ${ROOT_DIR}/test/sample/pg*txt ${BUILD_DIR}


# make sure software is freshly built.
(cd ${ROOT_DIR}/internal/mrapps/wc && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/wc.so wc.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/indexer && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/indexer.so indexer.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/mtiming && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/mtiming.so mtiming.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/rtiming && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/rtiming.so rtiming.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/jobcount && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/jobcount.so jobcount.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/early_exit && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/early_exit.so early_exit.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/crash && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/crash.so crash.go) || exit 1
(cd ${ROOT_DIR}/internal/mrapps/nocrash && go build $RACE -buildmode=plugin -o ${BUILD_DIR}/nocrash.so nocrash.go) || exit 1
(cd ${ROOT_DIR}/cmd/main && go build $RACE -o ${BUILD_DIR} mrcoordinator.go) || exit 1
(cd ${ROOT_DIR}/cmd/main && go build $RACE -o ${BUILD_DIR} mrworker.go) || exit 1
(cd ${ROOT_DIR}/cmd/main && go build $RACE -o ${BUILD_DIR} mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
(
  cd "${BUILD_DIR}" || exit 1
  ${BUILD_DIR}/mrsequential ${BUILD_DIR}/wc.so pg*txt || exit 1
)
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

(
  cd "${BUILD_DIR}" || exit 1
  maybe_quiet $TIMEOUT ./mrcoordinator pg*txt &
)
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/wc.so) &
(maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/wc.so) &
(maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/wc.so) &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
(
  cd "${BUILD_DIR}" || exit 1
  ${BUILD_DIR}/mrsequential ${BUILD_DIR}/indexer.so pg*txt || exit 1
)
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT ${BUILD_DIR}/mrcoordinator pg*txt &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/indexer.so &
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*
(
  cd "${BUILD_DIR}" || exit 1
  maybe_quiet $TIMEOUT ${BUILD_DIR}/mrcoordinator pg*txt &
)
sleep 1

maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/mtiming.so &
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait


#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

(
  cd "${BUILD_DIR}" || exit 1
  maybe_quiet $TIMEOUT ${BUILD_DIR}/mrcoordinator pg*txt &
)
sleep 1

maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/rtiming.so  &
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

(
  cd "${BUILD_DIR}" || exit 1
  maybe_quiet $TIMEOUT ${BUILD_DIR}/mrcoordinator pg*txt  &
)
sleep 1

maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/jobcount.so &
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/jobcount.so
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/jobcount.so &
maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -f mr-*

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(
  cd "${BUILD_DIR}" || exit 1
  maybe_quiet $TIMEOUT ${BUILD_DIR}/mrcoordinator ${BUILD_DIR}/pg*txt; touch $DF
) &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ${BUILD_DIR}/mrworker ${BUILD_DIR}/early_exit.so; touch $DF) &

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
(
  cd "${BUILD_DIR}" || exit 1
  ${BUILD_DIR}/mrsequential ${BUILD_DIR}/nocrash.so pg*txt || exit 1
)
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(
  cd "${BUILD_DIR}" || exit 1
  # 启动 coordinator，传相对路径的输入文件
  (maybe_quiet $TIMEOUT2 ./mrcoordinator pg*txt; touch mr-done) &
)
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT2 ${BUILD_DIR}/mrworker ${BUILD_DIR}/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ${BUILD_DIR}/mrworker ${BUILD_DIR}/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ${BUILD_DIR}/mrworker ${BUILD_DIR}/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 ${BUILD_DIR}/mrworker ${BUILD_DIR}/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
