API server to allow new apps to query existing database for sailing results.

Build the API server with gradle: 
```bash
./gradlew clean build fatJar
```

Copy the database dump to deploy/mysql/ then execute the following commands to 
bring up the server locally 

```bash
cd deploy
./deploy-local.sh
API server will be available on http://localhost:8080
``` 

Run the following commands to deploy to [hyper](https://hyper.sh/) (You need their CLI)
```bash
cd deploy
./deploy.sh
```

API should now be available on localhost:8080/

TODO: 
* API documentation
* Deploy to actual server