docker run --rm --network=host --env-file .env -i -t  -v "$PWD":/usr/src/app:Z -w /usr/src/app edcpy3 python "$@"
