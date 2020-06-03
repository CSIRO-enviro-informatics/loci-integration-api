# LOCI Integration API

## Description
This application provides a convenient interface for executing common functions across the whole LOCI system.

## Implementation
Uses Sanic Asynchronous HTTP Service with the Sanic-Restplus plugin to generate API endpoint, and to auto-document the SwaggerUI.

## Run

docker-compose is included for an containerized local deployment

## Test/Develop

run `docker-compose -f docker-compose.yml -f docker-compose.dev.yml up --build` to build in dev model and run a container for running tests or development

if running the label search engine, run
`docker-compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.es.yml up --build` 

if running the label search engine only (no sanic api),
`docker-compose -f docker-compose.es.yml up --build` 

if running the api with docker image from docker hub
`docker-compose -f docker-compose.yml -f docker-compose.useimage.yml up -d` 


## Known issues

If running the elasticsearch appliance throws up an error like:
```
initial heap size [2685456] not equal to maximum heap size [2147483648]; this can cause resize pauses and prevents mlockall from locking the entire heap
```

Ensure this is updated on the OS like so...
```
$ sudo sysctl -w vm.max_map_count=262144

```

## License
The license of this document is TBD

## Contacts
Project Technical Lead:  
**Jonathan Yu**  
*Senior Experimental Scientist*  
CSIRO Land & Water
<jonathan.yu@csiro.au>  
<http://orcid.org/0000-0002-2237-0091>

Lead Developer:  
**Ashley Sommer**  
*Software Engineer*  
CSIRO Land & Water
<ashley.sommer@csiro.au>  
<http://orcid.org/0000-0003-0590-0131>

Developer:  
**Ben Leighton**  
*Software Engineer*  
CSIRO Land & Water
<ben.leighton@csiro.au>  
