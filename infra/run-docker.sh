#!/usr/bin/env bash
set -ex
docker pull howinator/data-eng-postgres:0.1
docker run -d -p 32768:5432 --name data-eng howinator/data-eng-postgres