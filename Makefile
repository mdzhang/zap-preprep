run:
	docker run -d -p 11211:11211 --name memcached memcached:1.4.37 -m 1000

down:
	docker rm -f $$(docker ps -aqf name=memcached)
