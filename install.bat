@echo off

where docker-compose
if %ERRORLEVEL% neq 0 (
	echo Error: docker-compose is not installed.
	exit
)

cd Docker

echo Installing kafka (broker and kafka-ui)
docker-compose -f docker-kafka.yml up -d

echo Installing monitoring components(prometheus and grafana)
docker-compose -f docker-monitoring.yml up -d

:menu
echo Select Datahub Setup: 
echo [1] Docker/Podman 
echo [2] Local
set /p choice=Enter your choice (1/2): 

if "%choice%"=="" (
    echo "Invalid input."
    goto menu
)

if "%choice%"=="1" goto docker
if "%choice%"=="2" goto local

:docker
	cd ../scripts

	call buildImage.bat
	
	cd ../Docker
	echo Installing Datahub (datahub-app, adapter-1, adapter-2)
	docker-compose -f docker-datahub.yml up -d
	
exit

:local
	cd ../DatahubPOC
	echo Starting DatahubPOC
	start "Datahub" mvnw spring-boot:run
	
	cd ../Adapter
	echo Starting Adapter
	start "Adapter" mvnw spring-boot:run
	
exit