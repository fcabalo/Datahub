set dp=podman
where podman
if %ERRORLEVEL%==0 (
	set dp=podman
) else (
	where docker
	if %ERRORLEVEL%==0 (
		set dp=docker
	) else (
		echo Error: docker or podman is not installed.
		exit
	)
)

cd ../DatahubPOC
echo Packaging DatahubPOC
call mvnw clean package
echo Building datahub-app image
%dp% build -t datahub-app .

cd ../Adapter
echo Packaging Adapter
call mvnw clean package
echo Building adapter-app image
%dp% build -t adapter-app .