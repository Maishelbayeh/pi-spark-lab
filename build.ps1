param([string]$SparkHome = $env:SPARK_HOME)
$ErrorActionPreference = 'Stop'
if (-not $SparkHome) {
    $submit = Get-Command spark-submit.cmd -ErrorAction Stop
    $SparkHome = Split-Path (Split-Path $submit.Source -Parent) -Parent
}
$jars = Join-Path $SparkHome 'jars/*'
$classes = Join-Path $PSScriptRoot 'build/classes'
New-Item -ItemType Directory -Force -Path $classes | Out-Null
& java -cp $jars scala.tools.nsc.Main -usejavacp -d $classes (Join-Path $PSScriptRoot 'pi-estimator.scala')
if ($LASTEXITCODE -ne 0) { throw 'Scala compilation failed' }
& jar cf (Join-Path $PSScriptRoot 'build/pi-estimator.jar') -C $classes .
if ($LASTEXITCODE -ne 0) { throw 'JAR creation failed' }
Write-Host 'Scala Spark application built successfully.'
