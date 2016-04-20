# redis-tsdb 
The purpose of this project is to implement TSDB in Redis and expose as Data microservices. The project is largely inspired by [openTSDB](openstsdb.net), but with the purpose to look into realtime time serices processing and bringing the capabilities to microservices and the cloud. 

**Thoughts**
* Integrate with [Grafana](http://grafana.org/) for quick visualizations
* Implement R extensions for enabling data scientists  

## Getting Started
You'll need to have access to a [Redis](http://redis.io/topics/quickstart) server.

**Building**  
Prior to building, make sure your Redis server is running, otherwise the tests will fail.

```
$ git clone https://github.com/mborges-pivotal/redis-tsdb
$ cd redis-tsdb
$ ./mvnw clean install
``` 

### To run the application locally
The application is set to use a local Redis server in non-PaaS environments, and to take advantage of Pivotal CF's auto-configuration for services. To use a Redis service in PCF, simply create and bind a service to the app and restart the app. No additional configuration is necessary when running locally or in Pivotal CF.

```
$ ./mvnw spring-boot:run
```

Then go to the http://localhost:8080 in your browser

### Running on Cloud Foundry
Take a look at the manifest file for the recommended setting. Adjust them as per your environment.

The example below assumes you have a p-redis service with dedicated-vm plan in your Cloud Foundry Marketplace

```
$ cf create-service p-redis dedicated-vm redis-tsdb 
$ cf push
```


