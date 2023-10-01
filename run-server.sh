#!/bin/bash

set -xe

docker run --rm -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=passw0rd -e MYSQL_USER=testuser -e MYSQL_PASSWORD=passw0rd -e MYSQL_DATABASE=testdb mysql:8 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci \n
