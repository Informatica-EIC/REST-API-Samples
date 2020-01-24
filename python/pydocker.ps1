write-host "total args passed " $args.count
write-host "total args passed $args"

$stopwatch = [system.diagnostics.stopwatch]::startNew()
docker run --rm --network=host --env-file .env -i -t  -v ${PWD}:/usr/src/app:Z -w /usr/src/app edc_py_base:python38 python $args
$stopwatch.stop()

$elapsed = [math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
write-host "runDocker.ps1 - elapsed time = " $elapsed
