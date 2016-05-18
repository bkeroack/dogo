#!/bin/bash

protoc -I ./protos ./protos/dogo.proto --go_out=plugins=grpc:cmd
