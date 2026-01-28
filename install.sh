if ! [ -x "$(command -v docker-compose)" ]; then
	echo "Error: docker-compose is not installed."
	exit 1
fi

cd Docker

echo "Installing kafka (broker and kafka-ui)"
docker-compose -f docker-kafka.yml up -d

echo "Installing monitoring components(prometheus and grafana)"
docker-compose -f docker-monitoring.yml up -d

echo "Select Datahub Setup"
select option in "Docker/Podman" "Local"
do
    case $option in
        "Docker/Podman")
			cd ../scripts
			./buildImage
			
			cd ../Docker
			echo "Installing Datahub (datahub-app, adapter-1, adapter-2)"
			docker-compose -f docker-datahub.yml up -d
			
			break;;
        "Local")
			cd ../DatahubPOC
			echo "Starting DatahubPOC"
			nohup ./mvnw spring-boot:run &
			
			cd ../Adapter
			echo "Starting Adapter"
			nohup ./mvnw spring-boot:run &
			
			break;;
        *)
			echo "Choose only from the 2 options";
    esac
done