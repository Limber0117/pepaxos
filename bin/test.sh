#!/usr/bin/env bash

BINPATH=bin

# exit when any command fails
set -e

# commom flags
DATABASE="-database ./results.db"

# master flags
EPAXOS="-e=true"
DREPLY="-dreply=true"
EXEC="-exec=true"
THRIFTY="-thrifty=true"
BATCHWAIT="-batchwait 5"
LISTSIZE="-listSize 10000"
EXECTHREADS="-execThreads=64"
EXECPARALLEL="-execParallel=true"
WRITELATENCYTOLOG="-writelatencytolog=true"
WRITELATENCYTODB="-writelatencytodb=false"
NCLIENTS="-NC=1"
MASTERARGS="${DATABASE} ${LISTSIZE} ${EXECTHREADS} ${EXEC} ${DREPLY} ${EPAXOS} ${THRIFTY} ${BATCHWAIT} ${EXECPARALLEL} ${WRITELATENCYTOLOG} ${WRITELATENCYTODB} ${NCLIENTS}"


# client flags
# WRITES="-w 0"
CONFLICTS="-c 0"
REQUESTS="-q 9999999"
TIMEOUT="-timeout 20"
PROCS="-p 8"

CLIENTARGS="${REQUESTS} ${TIMEOUT} ${WRITES} ${CONFLICTS} ${PROCS}"

start_exp(){
    stop_all
    sleep 1
}

start_master(){
    echo "--- Starting Master"
    ${BINPATH}/master ${MASTERARGS} &
    sleep 1
}

start_server(){
    echo "--- Starting Replicas"
    ${BINPATH}/server -port 7070 &
    ${BINPATH}/server -port 7071 &
    ${BINPATH}/server -port 7072 &
    sleep 5
}

start_client(){
    echo "--- Starting Client"
    ${BINPATH}/client ${CLIENTARGS}
    sleep 5
}

stop_all(){
    sleep 1
    echo "--- Stoping all"
    killall server master client &
}

trap "stop_all; exit 255" SIGINT SIGTERM

start_exp
start_master
start_server
start_client

stop_all
