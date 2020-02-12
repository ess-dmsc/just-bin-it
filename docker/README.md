# Creating a Docker image

## Building, tagging and pushing
Build:
```
docker build --no-cache -t just_bin_it .
```

Tag:
```
docker tag just_bin_it screamingudder/just-bin-it:latest
```

Push:
```
docker push screamingudder/just-bin-it:latest
```

## Running
```
docker run -it -e PARAMETERS='--broker 169.254.98.29:9092 --config-topic hist_commands' just_bin_it
```

See the just-bin-it README for more information on choosing parameters.
