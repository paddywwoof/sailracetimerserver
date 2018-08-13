API server to allow new apps to query existing database for sailing results.

Copy the database dump to deploy/mysql/ then execute the following commands to 
bring up the server

```
./gradlew fatJar
cd deploy
docker-compose build && docker-compose up
``` 

API should now be available on localhost:8080/

TODO: 
* API documentation
* Deploy to actual server