release:
	./gradlew dockerBuildImage
	docker tag pkoperek/cloudsimplus-gateway:1.6 pkoperek/cloudsimplus-gateway:latest	
	docker push pkoperek/cloudsimplus-gateway:latest
	docker push pkoperek/cloudsimplus-gateway:1.6
