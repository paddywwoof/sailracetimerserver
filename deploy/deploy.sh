#!/usr/bin/env bash
docker-compose build
docker-compose push
hyper compose pull -f hyper-compose.yml
hyper compose up -f hyper-compose.yml -d --force-recreate
