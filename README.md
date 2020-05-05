# PePaxos

*PePaxos* is a novel approach to parallel execution in State Machine Replication (SMR) that benefits from generalized consensus to schedule parallel execution of independent SMR commands. The proposed concurrent scheduling algorithm was developed and integrated into Egalitarian Paxos and the prototype is based on the original *[ePaxos][1]* implementation.

[1]: https://github.com/efficient/epaxos/tree/morethan5

## Installation
This project requires Golang 1.13.1. To compile:

    make

## Usage

    make test
