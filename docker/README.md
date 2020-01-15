# Creating a Docker image

## Building
```
docker build -t just_bin_it  .
```

## Running
```
docker run -it -e PARAMETERS='--broker 169.254.98.29:9092 --config-topic hist_commands' just_bin_it
```

See the just-bin-it README for more information on choosing parameters.
